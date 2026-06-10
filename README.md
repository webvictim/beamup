# beamup

Bidirectional real-time file sync with [Teleport Beams](https://beams.run).

beamup keeps a local directory and a Beam's filesystem in sync — changes on either side propagate to the other in near-realtime. It manages the full beam lifecycle (create, sync, destroy) and is designed to recover gracefully from the inherent instability of ephemeral VMs.

## How it works

beamup runs two components:

- **Local CLI** (`beamup`) — watches your project directory via FSEvents, communicates with the remote agent over a persistent `tsh beams exec` pipe
- **Remote agent** (`beamup-agent`) — a small static binary deployed into the beam, watches the remote filesystem via inotify, relays changes back

Communication uses length-prefixed msgpack frames over stdin/stdout of the `tsh beams exec` process. No ports consumed, no direct IP needed.

## Install

```bash
./scripts/build.sh
```

This cross-compiles the agent for linux/arm64, embeds it into the CLI binary, and produces `target/release/beamup` — a single self-contained binary.

Requires [cross](https://github.com/cross-rs/cross) for the agent cross-compilation. To build just the CLI without embedding (for development):

```bash
cargo build
```

## Usage

```bash
# Create a beam, deploy the agent, and start syncing the current directory
beamup start

# Sync with an existing beam
beamup start --beam kinetic-vault

# Sync a specific directory
beamup start --path ~/projects/myapp

# Start sync and immediately drop into a console on the beam
beamup start --console

# One-way sync: push local to beam, then only pull back changes
beamup start --initial-sync local-to-beam --ongoing-sync beam-to-local

# Push-only sync (no changes pulled back from beam)
beamup start --initial-sync local-to-beam --ongoing-sync local-to-beam

# Check sync status
beamup status

# Run a command in the beam
beamup exec -- cargo test

# Stop syncing and destroy the beam
beamup down

# Stop syncing but keep the beam alive
beamup down --keep-beam
```

## Sync direction

By default, beamup syncs bidirectionally. You can control the direction independently for the initial sync and ongoing sync:

| Flag | Values | Default |
|------|--------|---------|
| `--initial-sync` | `local-to-beam`, `beam-to-local`, `bidirectional` | `bidirectional` |
| `--ongoing-sync` | `local-to-beam`, `beam-to-local`, `bidirectional` | `bidirectional` |

Common patterns:
- **Dev on beam**: `--initial-sync local-to-beam --ongoing-sync bidirectional` — push code up, then sync both ways
- **Build on beam**: `--initial-sync local-to-beam --ongoing-sync beam-to-local` — push code up, only pull back artifacts
- **Push-only mirror**: `--ongoing-sync local-to-beam` — beam is a read-only mirror of local

## Features

- **Bidirectional sync** — local edits push to beam, beam edits pull to local
- **Configurable direction** — one-way or bidirectional, independently for initial and ongoing sync
- **Progress bar** — real-time transfer progress with per-chunk updates, transfer rate, and ETA
- **Console mode** — `--console` drops you into a beam shell after sync completes
- **Near-realtime** — sub-second propagation via OS-native file watching
- **Conflict detection** — when both sides edit the same file, both versions are preserved (`.local.conflict` suffix) and the user is alerted
- **Respects .gitignore** — plus an optional `.beamignore` for additional exclusions
- **Atomic writes** — write-to-temp-then-rename prevents partial file corruption
- **Heartbeat monitoring** — detects beam death within 15 seconds
- **Large file chunking** — files over 64MB are split into chunks, compressed, and transferred in parallel

## Architecture

```
┌─────────────┐          tsh beams exec          ┌─────────────┐
│  beamup     │ ◄──── stdin/stdout pipe ────►    │ beamup-agent│
│  (macOS)    │     msgpack length-prefixed      │ (linux/arm64)│
│             │          frames                   │             │
│ FSEvents    │                                   │ inotify     │
│ watcher     │                                   │ watcher     │
└─────────────┘                                   └─────────────┘
```

## Project structure

```
crates/
├── beamup-protocol/   Shared message types, codec, hashing, ignore rules
├── beamup-cli/        Local CLI binary (macOS)
└── beamup-agent/      Remote agent binary (linux/arm64)
```

## Requirements

- `tsh` CLI with Beams support (authenticated via `tsh login`)
- Rust toolchain for building
- [cross](https://github.com/cross-rs/cross) for cross-compiling the agent (`cargo install cross --git https://github.com/cross-rs/cross`)
- Docker (required by `cross` to run the linux/arm64 build container)

## License

MIT
