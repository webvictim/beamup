#!/bin/bash
set -euo pipefail

# Beams may be either architecture, so the CLI embeds an agent for each and
# picks at deploy time based on `uname -m` on the beam. Building only one arch
# is supported (BEAMUP_ARCHS=x86_64) but the CLI then can't serve the other.
ARCHS="${BEAMUP_ARCHS:-x86_64 aarch64}"

target_for() { echo "$1-unknown-linux-musl"; }

# Prefer cross (containerized, reproducible). Fall back to a native cargo
# cross-compile, which needs the rustup target plus the musl linker that
# .cargo/config.toml points at.
have_cross() {
    command -v cross &> /dev/null && docker info &> /dev/null
}

have_native() {
    local arch="$1" target="$2"
    command -v "${arch}-linux-musl-gcc" &> /dev/null \
        && rustup target list --installed 2>/dev/null | grep -qx "$target"
}

build_agent() {
    local arch="$1"
    local target
    target="$(target_for "$arch")"

    echo "Building beamup-agent for $target..."
    if have_cross; then
        echo "  using cross."
        cross build --release --target "$target" -p beamup-agent
    elif have_native "$arch" "$target"; then
        echo "  cross unavailable; using native cargo cross-compile."
        # .cargo/config.toml only pins the aarch64 linker; set the rest here.
        local linker_var="CARGO_TARGET_$(echo "$target" | tr 'a-z-' 'A-Z_')_LINKER"
        env "$linker_var=${arch}-linux-musl-gcc" \
            cargo build --release --target "$target" -p beamup-agent
    else
        echo "error: no way to build the agent for $target." >&2
        echo "Install either:" >&2
        echo "  - cross, plus a running container engine:" >&2
        echo "      cargo install cross" >&2
        echo "  - or a native musl toolchain:" >&2
        echo "      brew install FiloSottile/musl-cross/musl-cross --with-${arch}" >&2
        echo "      rustup target add $target" >&2
        return 1
    fi

    local binary="target/$target/release/beamup-agent"
    if command -v "${arch}-linux-musl-strip" &> /dev/null; then
        "${arch}-linux-musl-strip" "$binary"
        echo "  stripped."
    fi
    echo "  agent: $binary ($(du -h "$binary" | cut -f1))"
}

for arch in $ARCHS; do
    build_agent "$arch"
done

echo ""
echo "Building beamup CLI (embedding agents: $ARCHS)..."
cargo build --release -p beamup-cli

CLI_BINARY="target/release/beamup"
CLI_SIZE=$(du -h "$CLI_BINARY" | cut -f1)
echo "CLI binary: $CLI_BINARY ($CLI_SIZE)"
