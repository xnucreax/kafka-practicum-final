#!/bin/bash
set -e

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

echo "Waiting for schema registry..."
until curl -sf "$SCHEMA_REGISTRY_URL/subjects" > /dev/null; do
  sleep 2
done

echo "Registering product raw schema..."
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\":\"JSON\",\"schema\":$(jq -Rs . /schemas/product-payload-schema.json)}" \
    "$SCHEMA_REGISTRY_URL/subjects/shop-products-raw-value/versions"
echo "\n"

echo "Registering product filtered schema..."
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\":\"JSON\",\"schema\":$(jq -Rs . /schemas/product-payload-schema.json)}" \
    "$SCHEMA_REGISTRY_URL/subjects/shop-products-filtered-value/versions"
echo "\n"

echo "Registering client request schema..."
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\":\"JSON\",\"schema\":$(jq -Rs . /schemas/client-request.json)}" \
    "$SCHEMA_REGISTRY_URL/subjects/client-requests-value/versions"
echo "\n"
