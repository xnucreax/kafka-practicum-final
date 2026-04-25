#!/usr/bin/env bash
set -euo pipefail

CONTAINER="analytics-service"

STATUS=$(docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null)
EXIT_CODE=$(docker inspect "$CONTAINER" --format '{{.State.ExitCode}}' 2>/dev/null)

if [ "$STATUS" = "running" ]; then
  echo "  [OK]   container is running"
else
  echo "  [FAIL] container status is '$STATUS' (expected 'running')"
  exit 1
fi

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "  [OK]   exit code is 0"
else
  echo "  [FAIL] exit code is $EXIT_CODE"
  exit 1
fi
