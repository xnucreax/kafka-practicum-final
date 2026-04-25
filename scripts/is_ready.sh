#!/usr/bin/env bash
set -euo pipefail

CONTAINER="analytics-service"
PASS=0
FAIL=0

ok()   { echo "  [OK]   $1"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }

echo "=== analytics-service readiness check ==="

# 1. Container exists
if ! docker inspect "$CONTAINER" &>/dev/null; then
  fail "container '$CONTAINER' not found"
  echo ""
  echo "Result: FAIL ($FAIL checks failed)"
  exit 1
fi
ok "container exists"

# 2. Container is running
STATUS=$(docker inspect "$CONTAINER" --format '{{.State.Status}}')
if [ "$STATUS" = "running" ]; then
  ok "container is running"
else
  fail "container status is '$STATUS' (expected 'running')"
fi

# 3. No unexpected restarts (allow 0 or 1 for initial startup)
RESTARTS=$(docker inspect "$CONTAINER" --format '{{.RestartCount}}')
if [ "$RESTARTS" -le 1 ]; then
  ok "restart count is acceptable ($RESTARTS)"
else
  fail "container has restarted $RESTARTS times — likely crashing"
fi

# 4. Exit code is 0 (not crashed)
EXIT_CODE=$(docker inspect "$CONTAINER" --format '{{.State.ExitCode}}')
if [ "$STATUS" = "running" ] || [ "$EXIT_CODE" -eq 0 ]; then
  ok "exit code OK ($EXIT_CODE)"
else
  fail "non-zero exit code: $EXIT_CODE"
fi

# 5. No fatal errors in recent logs
RECENT_LOGS=$(docker logs "$CONTAINER" --tail 100 2>&1)
if echo "$RECENT_LOGS" | grep -qiE "^[0-9]{4}/[0-9]{2}/[0-9]{2}.*FATAL|log\.Fatalf|fatal "; then
  fail "fatal error found in logs"
else
  ok "no fatal errors in logs"
fi

# 6. Analytics goroutine is alive: look for [analytics] log lines
if echo "$RECENT_LOGS" | grep -q '\[analytics\]'; then
  ok "analytics job goroutine is producing log output"
else
  # Service may still be initializing (connecting to Spark/Kafka/HDFS)
  STARTED_AT=$(docker inspect "$CONTAINER" --format '{{.State.StartedAt}}')
  UPTIME_SECS=$(( $(date +%s) - $(date -d "$STARTED_AT" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "${STARTED_AT%%.*}" +%s 2>/dev/null || echo "0") ))
  if [ "$UPTIME_SECS" -lt 60 ]; then
    ok "no [analytics] log yet but container just started (${UPTIME_SECS}s ago) — still initializing"
  else
    fail "[analytics] goroutine has not logged after ${UPTIME_SECS}s — may be stuck connecting to Spark/Kafka/HDFS"
  fi
fi

echo ""
echo "=== Result: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -eq 0 ]; then
  exit 0
else
  exit 1
fi
