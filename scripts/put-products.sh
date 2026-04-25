#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/binaries/put-product"

if [ ! -x "$BIN" ]; then
  echo "Binary not found. Run 'make bins' first." >&2
  exit 1
fi

for file in "$REPO_ROOT"/products/*.json; do
  echo "Putting product: $file"
  "$BIN" --path "$file"
done
