#!/bin/sh

NETWORK="${COMPOSE_PROJECT_NAME:-module-final}_default"

docker run --rm \
  --network "$NETWORK" \
  -e PGPASSWORD=postgres \
  debezium/postgres:16 \
  psql -h postgres -U postgres -d postgres -c "
SELECT product_id, name, category, brand, sku, price, stock
FROM products
ORDER BY name;"
