#!/bin/sh

set -e # Exit early if any commands fail

# Resolve the absolute path of this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Build output directory
mkdir -p "$REPO_ROOT/bin"

# Build
go build -o "$REPO_ROOT/bin/revtorrent" \
    "$REPO_ROOT/cmd/revtorrent"

# Run binary with passed arguments
exec "$REPO_ROOT/bin/revtorrent" "$@"
