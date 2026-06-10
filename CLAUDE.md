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

The build.rs auto-embeds the agent if found at `target/aarch64-unknown-linux-musl/release/beamup-agent`. Override with `BEAMUP_AGENT_PATH`. If no agent is found at build time, the CLI falls back to runtime lookup.

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

Protocol version 3. Messages are msgpack-encoded, length-prefixed (4-byte big-endian). Key message flow:

1. CLI sends `Hello` (with sync directions) → Agent replies `HelloAck`
2. CLI sends `FileManifest` → Agent replies `SyncPlan` (what to push/pull)
3. Transfers happen (tar batches for small files, chunked SCP for large)
4. Both sides exchange `ManifestAck`
5. Ongoing: `FileChanged`/`FileContent`/`FileDeleted` messages in both directions

## Conventions

- Use `tracing` for logging (info/debug/warn), not println
- Temp files use `.beamup-tmp` extension (auto-ignored by sync)
- Large files (>chunk_size, default 64MB) are split, lz4-compressed per-chunk, transferred in parallel
- `.git/index` and `.git/index.lock` are never synced (platform-specific)
- All `tsh` invocations go through `beam::tsh_command()` / `beam::tsh_command_sync()` helpers (prepend `-i` and `--proxy` when configured)
- SCP and exec operations retry up to 3 times with exponential backoff on failure
- `--oneshot` mode: handshake + initial sync only, then exit (no watcher, no ongoing loop)
