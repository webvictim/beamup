# Changelog

All notable changes to beamup, newest first. Dates are release dates.

Each version links to its full commit range. Upgrading is just replacing the
`beamup` binary: it deploys the matching agent to the beam itself. Entries
call out when more than that is needed.

## [v0.2.1] — 2026-11-10

### Changed

- **A relative `--remote-path` is now under `/home/beams`.** `--remote-path git`
  syncs to `/home/beams/git`. Previously a relative path resolved against the
  agent's working directory, which isn't guaranteed to be the beam's home. If
  you passed a relative path that worked, it most likely still points to the
  same place. Absolute paths are unchanged.

### Fixed

- **`~` in `--local-path` and `--remote-path` now works when your shell
  doesn't expand it.** Neither path passes through a shell after you type it,
  so a quoted `'~/project'`, or `--local-path=~/project` (which shells leave
  alone), was taken literally. Locally that failed with a bare "No such file
  or directory"; on the beam it created a directory actually named `~`. A
  local `~` now expands to your home directory, and a remote `~` to
  `/home/beams`. `~otheruser` is rejected with an error asking for an absolute
  path, rather than syncing to the wrong place.
- **Path errors name the path.** When `--local-path` can't be resolved, or the
  agent can't create its remote directory, the error now says which path.

## [v0.2.0] — 2026-10-06

### Added

- **The beam agent now stays running between sessions, so re-syncing is about
  twice as fast.** On beams that run beam-init (all new beams), beamup starts
  its agent as a `beamctl` service instead of a process tied to one
  `tsh beams exec` connection. Reconnecting skips the agent upload, and the
  agent only re-reads files that changed since it last synced, rather than
  hashing the whole tree again. A repeat `--oneshot` sync of a 3,000-file
  project drops from about 14s to about 6s. The first sync to a new beam takes
  as long as before.

  The agent keeps running after Ctrl-C and after `--oneshot`, until
  `beamup down --keep-beam` stops it or the beam expires. Beams without
  `beamctl` work exactly as before. To opt out on beams that have it, pass
  `--no-persistent-agent` or set `BEAMUP_PERSISTENT_AGENT=0`.
- **`beamup status` reports the agent.** When the session has a persistent
  agent, `status` shows whether it's running, and `status --json` gains an
  `agent` field. Existing fields are unchanged.
- **Agent startup failures say why.** If the agent fails to start, the
  handshake error now includes the agent service's `beamctl logs` output
  alongside its stderr.

### Changed

- **Fewer round trips before syncing starts.** One probe of the beam replaces
  three separate `tsh beams exec` calls, at about 2s each. It checks the
  architecture, whether `beamctl` is there, and whether the right agent is
  already uploaded. Agents now live at a content-addressed
  `/tmp/beamup-agent-<hash>` and are only uploaded when that exact build is
  missing.
- **A second session for the same directory takes over from the first.**
  Starting `beamup sync` for a beam and remote path that another session is
  already syncing now disconnects the older session ("connection to beam
  lost"). Previously both ran side by side with separate agents.
- **`beamup down --keep-beam` stops the agent** on the beam as well as ending
  the local session.

### Fixed

- **Pushing files could truncate them, locally and on the beam.** During the
  initial sync, and when pushing large changed files during a live session,
  the agent could report a file back to your machine while it was still being
  written on the beam. The local copy was then overwritten with the partial
  file, and the truncated copy synced back to the beam. In testing, 2 MB files
  ended up about 40 KB. Sometimes a stray `.local.conflict` file appeared too.
  This has been possible since v0.1.0. The agent now ignores its own incoming
  transfers until they finish. If you've synced with an earlier version,
  check larger files you pushed for truncation.
- **Agents no longer pile up on the beam.** An agent whose session ended
  (Ctrl-C, `--oneshot`, a dropped connection) never actually exited, so every
  sync left another one running. Agents from earlier versions may still be
  running on existing beams. To clear them:
  `tsh beams exec <beam> -- pkill -f '/tmp/beamup-agent --serve'`.
- **Large-file transfers from concurrent sessions no longer break each
  other.** Deploying the agent used to wipe `/tmp/beamup-xfer`, deleting the
  chunks of any large file another session was transferring at the time.

## [v0.1.2] — 2026-09-14

### Fixed

- **beamup now works with x86_64 beams.** The agent was only ever built for
  aarch64, so syncing to an x86_64 beam failed with
  `expected HelloAck, got: None` and no hint as to why. beamup now ships both
  agents and picks the one matching the beam's architecture at deploy time.
- **Agent startup errors are no longer swallowed.** Whatever the agent prints
  to stderr is now logged and included in a failed handshake's error message.
  Before, an agent that crashed on startup left no trace.
- **`beamup --version` reports the right version.** The v0.1.1 binaries
  reported `0.1.0`. Releases now check that the version matches the tag
  before publishing.

### Changed

- **License changed from MIT to Apache-2.0.**
- **Release assets are just the four CLI binaries.** The separate
  `beamup-agent` download is gone; both agents are embedded in the `beamup`
  binary.
- **Documented `--concurrency` tuning.** The README and SKILL.md now give
  throughput figures, and recommend `-c 16` for automated use.

## [v0.1.1] — 2026-06-10

### Added

- **Machine ID support.** `--identity`/`-i` and `--proxy` let beamup run with
  a `tbot` identity file instead of an interactive `tsh login`, for headless
  and automated use.
- **`--oneshot` mode** for `start` and `sync`: perform the initial sync, print
  the beam name to stdout and exit. With `-q`, this makes beamup a building
  block for scripts and agents:
  `BEAM=$(beamup -q start --oneshot --local-path ~/project)`.
- **Automatic retries.** scp, exec and tar transfers retry up to three times
  with exponential backoff, so transient `tsh` failures no longer abort a sync.
- **Homebrew installation**: `brew install webvictim/tap/beamup`.

### Known issues

- The binaries report `0.1.0` from `beamup --version`. Fixed in v0.1.2.

## [v0.1.0] — 2026-06-10

First release. beamup keeps a local directory and a Teleport Beam's
filesystem in sync in near-realtime, over `tsh beams exec` with no ports or
direct IP needed. It deploys a small agent to the beam, embedded in the
`beamup` binary.

- `beamup start` creates a beam and syncs, `sync` attaches to an existing
  one, and `exec`, `status` and `down` manage it.
- Sync direction can be set separately for the initial and ongoing sync with
  `--initial-sync` and `--ongoing-sync` (`local-to-beam`, `beam-to-local`,
  `bidirectional`). `--console` drops you into a shell on the beam once the
  initial sync is done.
- Small files go in batched tar streams. Files over 64 MB (`--chunk-size`) are
  split, LZ4-compressed and transferred in parallel. A progress bar shows the
  initial sync.
- `.gitignore` and `.beamignore` are respected. `.git/index` is never synced,
  since its format is platform-specific.
- When both sides change the same file, the local version is kept as
  `<file>.local.conflict`. A heartbeat detects a dead beam within 15 seconds.

### Known issues

- Only aarch64 beams are supported. Fixed in v0.1.2.

[v0.2.1]: https://github.com/webvictim/beamup/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/webvictim/beamup/compare/v0.1.2...v0.2.0
[v0.1.2]: https://github.com/webvictim/beamup/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/webvictim/beamup/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/webvictim/beamup/releases/tag/v0.1.0
