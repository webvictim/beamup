---
description: Syncing files to Teleport Beams with beamup — a real-time bidirectional file sync tool. Use when syncing a local directory to a beam for development, builds, or running code remotely. Covers sync setup, direction control, oneshot mode for agents, Machine ID authentication, and troubleshooting.
---

# beamup skill

beamup syncs a local directory to a Teleport Beam in real-time. It deploys a small agent binary to the beam, then keeps files in sync over a `tsh beams exec` pipe. Changes propagate in sub-second.

## Prerequisites

- `tsh` CLI authenticated (`tsh login`) — or a Machine ID identity file for headless use
- beamup built: `./scripts/build.sh` (produces a single `target/release/beamup` binary with agent embedded)
- Or install via Homebrew: `brew install webvictim/tap/beamup`

## Agent / automation usage (oneshot mode)

When an agent needs to provision a beam with a file structure before running commands on it, use `-q` and `--oneshot`. This suppresses all log output and prints only the beam name to stdout after the initial sync completes:

```bash
# Create a beam, sync files, print beam name, exit
BEAM=$(beamup -q start --oneshot --local-path ~/projects/myapp)
tsh beams exec "$BEAM" -- make build
```

For headless environments (no interactive `tsh login`), combine with Machine ID:

```bash
BEAM=$(beamup -q -i /path/to/identity --proxy cluster.example.com:443 \
  start --oneshot --local-path ~/projects/myapp)
tsh -i /path/to/identity --proxy cluster.example.com:443 beams exec "$BEAM" -- make build
```

Key flags for agent use:
- `-q` — suppress all log output (only errors to stderr)
- `--oneshot` — exit after initial sync, print beam name to stdout
- `-i <path>` — use a Machine ID identity file instead of interactive login
- `--proxy <addr>` — Teleport proxy address (required with `-i`)

## Interactive usage

```bash
# Sync current directory to a new beam (bidirectional)
beamup start

# Sync and immediately get a shell on the beam
beamup start --console

# Sync a specific directory
beamup start --local-path ~/projects/myapp

# Use an existing beam
beamup start --beam kinetic-vault

# Stop sync and destroy the beam
beamup down
```

## Sync direction control

Control initial and ongoing sync directions independently:

```bash
# Push code up, then only pull back results
beamup start --initial-sync local-to-beam --ongoing-sync beam-to-local

# Push-only (beam is a read-only mirror)
beamup start --initial-sync local-to-beam --ongoing-sync local-to-beam

# Full bidirectional (default)
beamup start
```

Values: `local-to-beam`, `beam-to-local`, `bidirectional`

## Key flags

| Flag | Purpose |
|------|---------|
| `-q` / `--quiet` | Suppress all log output (errors only) |
| `--oneshot` | Exit after initial sync, print beam name to stdout |
| `-i` / `--identity <path>` | Teleport identity file (Machine ID / tbot) |
| `--proxy <addr>` | Teleport proxy address (required with `--identity`) |
| `--console` | Launch `tsh beams console` after initial sync |
| `--initial-sync <dir>` | Direction for initial file reconciliation |
| `--ongoing-sync <dir>` | Direction for ongoing change propagation |
| `--concurrency N` | Max parallel SCP transfers (default: 8) |
| `--chunk-size N` | Chunk size in MB for large files (default: 64) |
| `-v` / `--verbose` | Show debug-level transfer logs |

## Commands

```bash
beamup start [OPTIONS]     # Create beam + start syncing
beamup sync [OPTIONS]      # Sync with existing beam (no create)
beamup exec -- <cmd>       # Run command in synced beam
beamup status              # Show sync status
beamup down [--keep-beam]  # Stop sync, optionally keep beam alive
```

## How files are synced

- Files under 64KB: sent inline over the protocol pipe
- Files under chunk-size (64MB): batched into tar streams, transferred in parallel
- Files over chunk-size: split into chunks, lz4-compressed, SCP'd in parallel, reassembled by agent

## Ignoring files

beamup respects `.gitignore` and an optional `.beamignore` file (same syntax). Additionally, `.git/index` and `.git/index.lock` are always excluded (platform-specific binary format).

## Troubleshooting

- **"agent protocol version mismatch"** — Rebuild and redeploy the agent (`./scripts/build-agent.sh`)
- **"beam not found"** — Check `tsh beams ls`; beams expire after 24h
- **git segfault after sync** — Run `git status` to rebuild the index; this was caused by syncing .git/index cross-platform (now fixed)
- **Progress bar not moving** — Large files update per-chunk (64MB); very large files with few chunks may appear slow

## Remote directory

Files sync to `/home/beams/sync` on the beam by default (override with `--remote-path`). Connect to the beam with:

```bash
tsh beams console <beam-name>
cd ~/sync
```

## Adding this skill to Claude Code

Add to your Claude Code settings (`.claude/settings.json` or global):

```json
{
  "skills": [
    {
      "path": "/path/to/beamup/SKILL.md"
    }
  ]
}
```

Or symlink into your skills directory:

```bash
mkdir -p ~/.claude/skills/beamup
ln -s /path/to/beamup/SKILL.md ~/.claude/skills/beamup/SKILL.md
```
