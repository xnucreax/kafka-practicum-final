#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/binaries/block-tags"

if [ ! -x "$BIN" ]; then
  echo "Binary not found. Run 'make bins' first." >&2
  exit 1
fi

TAG="${1:?Usage: block-tags.sh <tag> [true|false]}"
BLOCKED="${2:-true}"

"$BIN" --tag "$TAG" --blocked "$BLOCKED"
