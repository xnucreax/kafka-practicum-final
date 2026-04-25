SCHEMA_REGISTRY_URL="https://rc1a-4ikju28p83gcc0u6.mdb.yandexcloud.net:443"
# USERNAME="<some-username>"
# PASSWORD="<some-password>"

echo "Registering product schema..."
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\":\"JSON\",\"schema\":$(jq -Rs . product-schema.json)}" \
    $SCHEMA_REGISTRY_URL/subjects/product-value/versions
echo "\n"

echo "Registering client request schema..."
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\":\"JSON\",\"schema\":$(jq -Rs . client-request-schema.json)}" \
    $SCHEMA_REGISTRY_URL/subjects/client-request-value/versions
echo "\n"
