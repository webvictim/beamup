#!/bin/bash
# Verify the workspace version matches the release tag.
#
# `beamup --version` comes from CARGO_PKG_VERSION (clap's #[command(version)]),
# which reads Cargo.toml — not the git tag. Tagging v0.1.1 without bumping
# Cargo.toml ships a binary that reports the old version, which is exactly what
# happened to v0.1.1. This is run by the release workflow to prevent a repeat.
set -euo pipefail

TAG="${1:-}"
if [ -z "$TAG" ]; then
    echo "usage: $0 <tag>   (e.g. v0.1.2)" >&2
    exit 2
fi

# Accept the tag with or without a leading "v".
TAG_VERSION="${TAG#v}"

CARGO_VERSION="$(
    awk -F'"' '/^\[workspace\.package\]/{p=1; next}
               /^\[/{p=0}
               p && /^version[[:space:]]*=/{print $2; exit}' Cargo.toml
)"

if [ -z "$CARGO_VERSION" ]; then
    echo "error: could not read version from [workspace.package] in Cargo.toml" >&2
    exit 1
fi

if [ "$TAG_VERSION" != "$CARGO_VERSION" ]; then
    cat >&2 <<EOF
error: version mismatch between git tag and Cargo.toml
  tag:        $TAG (version $TAG_VERSION)
  Cargo.toml: $CARGO_VERSION

The released binary reports the Cargo.toml version, so releasing this would ship
a binary claiming to be $CARGO_VERSION under a $TAG_VERSION release.

Fix: set version = "$TAG_VERSION" in [workspace.package] in Cargo.toml, commit,
then move the tag to that commit.
EOF
    exit 1
fi

echo "version OK: tag $TAG matches Cargo.toml $CARGO_VERSION"
