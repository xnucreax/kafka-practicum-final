#!/bin/sh

NETWORK="${COMPOSE_PROJECT_NAME:-module-final}_default"

docker run --rm \
  --network "$NETWORK" \
  -e PGPASSWORD=postgres \
  debezium/postgres:16 \
  psql -h postgres -U postgres -d postgres -c "
SELECT query
FROM client_requests
ORDER BY query;"
