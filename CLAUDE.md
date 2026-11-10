# CLAUDE.md

## Project overview

beamup is a bidirectional real-time file sync tool for Teleport Beams. It syncs a local directory with a remote beam VM over `tsh beams exec` pipes using msgpack-framed messages.

## Build

```bash
./scripts/build.sh   # Full release build (cross-compiles agent, embeds in CLI)
cargo build          # Dev build (CLI only, uses agent from target/ or embedded)
```

## Test

```bash
cargo test -p beamup-protocol  # Unit tests (ignore doctest failures — rustdoc path issue)
cargo build -p beamup-cli -p beamup-agent  # Verify both compile
```

## Run (development)

```bash
cargo build && ./target/debug/beamup start -v --local-path ~/my-project
```

## Run (oneshot / headless)

```bash
# Oneshot mode: sync files, print beam name to stdout, exit
beamup -q start --oneshot --local-path ~/project

# With Machine ID identity (no interactive tsh login needed)
beamup -q -i /path/to/identity --proxy cluster:443 start --oneshot --local-path ~/project
```

The build.rs auto-embeds an agent per architecture, if found at `target/{x86_64,aarch64}-unknown-linux-musl/release/beamup-agent`. Override an individual arch with `BEAMUP_AGENT_PATH_X86_64` / `BEAMUP_AGENT_PATH_AARCH64` (plain `BEAMUP_AGENT_PATH` still overrides the host-native arch). If no agent is found at build time, the CLI falls back to runtime lookup.

**Persistent agent.** When the beam has `beamctl` (beam-init), `Beam::deploy_agent` runs the agent as a beamctl service, `beamup-<hash(remote_dir)>`, labelled `app=beamup`. It is started with `--serve --listen /tmp/beamup-<hash>.sock`, and each session spawns `beamup-agent --connect <sock>` over `tsh beams exec` as a byte relay, so the protocol is unchanged. The service persists until `down --keep-beam` and caches file hashes between sessions. Without beamctl, or with `--no-persistent-agent` / `BEAMUP_PERSISTENT_AGENT=0`, the agent serves one session over the exec pipe as before. Service processes get cwd `/home/beams` and a minimal env (no `BEAM_ID`/`TELEPORT_*`), and `beamctl start` refuses an existing name even when it has exited, so always `stop --prune` first.

**Beam architecture.** Beams are not all the same arch — `Beam::deploy_agent` runs `uname -m` on the beam and picks the matching embedded agent. Deploying a mismatched agent makes it die with `Exec format error` on exec, which surfaces as a handshake failure, so don't hardcode a target triple in the deploy path. The arch comes from the single probe exec (`probe_beam`), which also reports beamctl, the agents already present and the service state. Agents live at the content-addressed `/tmp/beamup-agent-<hash>`; use `beam::agent_remote_path()` rather than hardcoding a path.

## Releasing

Releases are cut by pushing a `vX.Y.Z` tag; `.github/workflows/release.yml`
checks the tag against `Cargo.toml`, builds both agents and the four CLI
binaries, and publishes the release.

1. Bump `version` in `[workspace.package]` in `Cargo.toml` (and `Cargo.lock`)
   to match the tag. `beamup --version` reads `Cargo.toml`, and
   `scripts/check-version.sh` fails the release if they disagree.
2. Add the version's section to the top of `CHANGELOG.md` (newest first,
   Added / Changed / Fixed, plus its compare link at the bottom). Write it for
   users rather than paraphrasing commit subjects: describe user-visible impact
   and the symptom a fix cures, and say when an entry supersedes an earlier
   one.
3. Tag and push, then set the GitHub release body to that section with
   `gh release edit vX.Y.Z --notes-file <file>`, followed by a
   `**Full Changelog**: <compare link>` line. Don't leave releases with only
   the auto-generated compare link.

   **Unwrap first.** `CHANGELOG.md` is hard-wrapped to ~80 columns, but a
   release body is free text and those breaks read badly there. Pipe the
   section through `tools/unwrap-md.py`, which joins each bullet or paragraph
   onto one line and leaves headings, lists, fenced code and indented
   sub-paragraphs alone. Its tests (`python3 tools/unwrap_md_test.py`, also
   run by the release workflow) cover the real `CHANGELOG.md` sections, so run
   them after editing either.
4. Update `Formula/beamup.rb` in the homebrew-tap repo: `version` and the
   `sha256` of each of the four release binaries. Commit as
   `beamup X.Y.Z: <summary>`.

## Architecture

- `crates/beamup-protocol/` — Shared types, codec, hashing, ignore rules, compression
- `crates/beamup-cli/` — Local CLI binary. Key files:
  - `syncer.rs` — SyncEngine: handshake, initial sync, main event loop
  - `transfer.rs` — TransferPool: parallel SCP transfers (tar batches + chunked large files)
  - `progress.rs` — Global progress bar integration with tracing
  - `watcher.rs` — FSEvents filesystem watcher
  - `commands/` — CLI subcommands (start, sync, exec, down, status)
- `crates/beamup-agent/` — Remote agent binary (Linux). Key files:
  - `syncer.rs` — Agent main loop, manifest handling, watch events
  - `watcher.rs` — inotify filesystem watcher

## Protocol

Protocol version 4. Messages are msgpack-encoded, length-prefixed (4-byte big-endian). Key message flow:

1. CLI sends `Hello` (with sync directions and `--exclude` patterns) → Agent replies `HelloAck`
2. CLI sends `FileManifest` → Agent replies `SyncPlan` (what to push/pull)
3. Transfers happen (tar batches for small files, chunked SCP for large)
4. Both sides exchange `ManifestAck`
5. Ongoing: `FileChanged`/`FileContent`/`FileDeleted` messages in both directions

## Conventions

- Use `tracing` for logging (info/debug/warn), not println
- Temp files use `.beamup-tmp` extension (auto-ignored by sync)
- Large files (>chunk_size, default 64MB) are split, lz4-compressed per-chunk, transferred in parallel
- `.git/index` and `.git/index.lock` are never synced (platform-specific)
- `.tsh/`, `.kube/` and `.cache/` are never synced. A tsh profile only works with the private key held by its own beam's tbot, so a copied one breaks every Beam command with `tls: error decrypting message`; a kubeconfig points at that profile; caches are big and platform-specific. Built-ins live in `BUILTIN_PATTERNS` in `ignore.rs` and are checked before `--exclude`, `.gitignore` and `.beamignore`, so `!` can't undo them
- `--exclude` patterns travel to the agent in `Hello`, and both sides build `IgnoreRules::with_excludes` from them. The agent's watcher thread only has the base rules (a persistent agent starts it before any client), so `handle_watch_event` filters its events against the session's rules
- All `tsh` invocations go through `beam::tsh_command()` / `beam::tsh_command_sync()` helpers (prepend `-i` and `--proxy` when configured)
- SCP and exec operations retry up to 3 times with exponential backoff on failure
- `--oneshot` mode: handshake + initial sync only, then exit (no watcher, no ongoing loop); a persistent agent stays running
- The agent's inotify watcher runs on a plain `std::thread`, not `spawn_blocking`: tokio's runtime shutdown waits for blocking tasks, which kept agents alive after their session ended
