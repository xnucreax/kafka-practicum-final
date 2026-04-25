#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO_ROOT/put-product"

if [ ! -x "$BIN" ]; then
  echo "Building put-product..."
  cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/put-product
fi

for file in "$REPO_ROOT"/products/*.json; do
  echo "Putting product: $file"
  "$BIN" --path "$file"
done
