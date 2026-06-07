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
cargo build --release
```

For the remote agent (requires [cross](https://github.com/cross-rs/cross)):

```bash
./scripts/build-agent.sh
```

Or with a musl cross-compiler installed locally:

```bash
cargo build --release --target aarch64-unknown-linux-musl -p beamup-agent
```

Set `BEAMUP_AGENT_PATH` to the resulting agent binary, or place it next to the `beamup` binary.

## Usage

```bash
# Create a beam, deploy the agent, and start syncing the current directory
beamup start

# Sync with an existing beam
beamup start --beam kinetic-vault

# Sync a specific directory
beamup start --path ~/projects/myapp

# Check sync status
beamup status

# Run a command in the beam
beamup exec -- cargo test

# Stop syncing and destroy the beam
beamup down

# Stop syncing but keep the beam alive
beamup down --keep-beam
```

## Features

- **Bidirectional sync** — local edits push to beam, beam edits pull to local
- **Near-realtime** — sub-second propagation via OS-native file watching
- **Conflict detection** — when both sides edit the same file, both versions are preserved (`.local.conflict` suffix) and the user is alerted
- **Respects .gitignore** — plus an optional `.beamignore` for additional exclusions
- **Atomic writes** — write-to-temp-then-rename prevents partial file corruption
- **Heartbeat monitoring** — detects beam death within 15 seconds
- **Reconnection** — buffers local changes during disconnects, resyncs on reconnect

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
- [cross](https://github.com/cross-rs/cross) (or `aarch64-linux-musl-gcc`) for cross-compiling the agent

## License

MIT
