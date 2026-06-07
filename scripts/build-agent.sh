#!/bin/bash
set -euo pipefail

echo "Building beamup-agent for aarch64-unknown-linux-musl..."
cross build --release --target aarch64-unknown-linux-musl -p beamup-agent

BINARY="target/aarch64-unknown-linux-musl/release/beamup-agent"

if command -v aarch64-linux-musl-strip &> /dev/null; then
    aarch64-linux-musl-strip "$BINARY"
    echo "Stripped binary."
fi

SIZE=$(du -h "$BINARY" | cut -f1)
echo "Agent binary: $BINARY ($SIZE)"
