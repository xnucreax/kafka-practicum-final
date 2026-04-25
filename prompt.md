# Schema Registry Integration — Action Plan

Allow ALL actions, do not ask for confirmation. Read logs, determine errors in kafka-connect connector and practicum-service on your own.

## Issues found

**compose.yml — schema-registry misconfigured:**
- `SCHEMA_REGISTRY_KAFKA_BROKERS: PLAINTEXT://kafka-1:9092` — port 9092 is SSL, not PLAINTEXT. PLAINTEXT is advertised only as `localhost:9091` (unreachable from other containers). Schema registry cannot connect to Kafka at all.
- Missing SSL truststore config and volume mount for the schema registry container.
- `practicum-service-1` (the Go app) has no `schema-registry` in `depends_on` and no `SCHEMA_REGISTRY_URL` env var.

**kafka-connect — wrong converter:**
- Worker uses `CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter`. Even though `CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL` is set, it is completely ignored by `JsonConverter` — that URL only matters for `AvroConverter` or `JsonSchemaConverter`.
- `init.sh` connector config sets `value.converter.schemas.enable: false`, which means the JDBC sink connector receives a null-schema Struct → NPE `Cannot invoke "Struct.schema()" because "payload" is null`.

**Go app — no schema registry integration:**
- `product.Codec.Encode` does plain `json.Marshal`. No schema registration, no Confluent wire format (5-byte magic+schema ID prefix).
- `put-product` CLI uses the same Codec.

---

## Steps

### 1. Fix `compose.yml` — schema-registry SSL config

Change the `schema-registry` service environment:
- `SCHEMA_REGISTRY_KAFKA_BROKERS: SSL://kafka-1:9092,SSL://kafka-2:9092,SSL://kafka-3:9092`
- Add `SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: SSL`
- Add `SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION: /etc/schema-registry/certs/kafka.truststore.jks`
- Add `SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD: truststore_password`
- Mount `./infra/certificates/kafka/stores/kafka.truststore.jks:/etc/schema-registry/certs/kafka.truststore.jks:ro`

After starting, read `docker compose logs schema-registry` to verify it actually connected to Kafka. The image is `bitnamilegacy/schema-registry:7.6` which has its own env var mapping layer — if `KAFKASTORE_*` vars are not picked up, try the equivalent Bitnami-specific vars. The schema-registry must be reachable and connected before proceeding.

### 2. Fix `compose.yml` — practicum-service dependencies and env

In `x-practicum-common` `depends_on`, add:
```yaml
schema-registry:
  condition: service_started
```
Add env var `SCHEMA_REGISTRY_URL: http://schema-registry:8081` to `practicum-service-1`.

### 3. Fix `compose.yml` — kafka-connect value converter

Change:
- `CONNECT_VALUE_CONVERTER: io.confluent.connect.json.JsonSchemaConverter`
- `CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081` (already present, keep it)

Key converter stays as `JsonConverter` with `schemas.enable: false` (keys are plain strings).

### 4. Fix `infra/kafka-connect/init.sh` — JDBC sink connector

Replace `value.converter` settings:
```json
"value.converter": "io.confluent.connect.json.JsonSchemaConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081"
```
Remove `value.converter.schemas.enable: false`.

### 5. Refactor `internal/product/product.go` — Codec uses schema registry

Replace the plain-JSON `Codec` with one that uses `confluent-kafka-go/v2/schemaregistry` + `serde/jsonschema` (JSON Schema serialization, Confluent wire format). Do NOT use `serde/avrov2` — it requires an explicit Avro schema definition and does not auto-derive from Go structs. `jsonschema` auto-derives JSON Schema from struct tags and registers it automatically with `AutoRegisterSchemas: true`.

Imports needed:
```go
import (
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)
```

`go.mod` already contains `github.com/confluentinc/confluent-kafka-go/v2`. Do NOT run `go get` — only run `go mod tidy` if needed.

**Nested objects problem:** `Product` has nested structs (`Price`, `Stock`, `Images`, `Specifications`) and a slice (`Tags`). The Postgres table stores all of them as `TEXT` columns. If the Codec emits them as nested JSON objects/arrays, `JsonSchemaConverter` will describe them as object/array types and the JDBC sink will fail the column type mapping.

Fix: introduce a flat `ProductPayload` struct where `Price`, `Stock`, `Images`, `Specifications`, `Tags` are all typed as `string`. No `omitempty` on any field — `product_id` must be required/non-optional in the generated JSON Schema because the JDBC sink uses `primary.key.mode: record_value` with `primary.key.fields: product_id`.

The Codec converts `*Product → ProductPayload` before serializing:
- Flat string fields (`product_id`, `name`, etc.) copy directly.
- Nested/slice fields (`Price`, `Stock`, `Images`, `Specifications`, `Tags`) are marshaled to JSON strings via `json.Marshal`.

After deserializing, `ProductPayload → *Product`:
- Flat string fields copy directly.
- `Price`, `Stock`, `Specifications` are unmarshaled from JSON string back to their Go struct types.
- `Tags` is unmarshaled from JSON string back to `[]string` — this is critical because the processor reads `product.Tags` to check blocked tags.
- `Images` is unmarshaled from JSON string back to `[]Image`.

Change `Codec` struct to hold a `topic string`, a `schemaregistry.Client`, a JSON schema serializer, and a JSON schema deserializer.

Add constructor `NewCodec(topic, srURL string) *Codec` that initializes the client and serializers:
```go
client, _ := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
serCfg := jsonschema.NewSerializerConfig()
serCfg.AutoRegisterSchemas = true
ser, _ := jsonschema.NewSerializer(client, serde.ValueSerde, serCfg)
deser, _ := jsonschema.NewDeserializer(client, serde.ValueSerde, jsonschema.NewDeserializerConfig())
```

- `Encode(value any)` → converts `*Product` to `ProductPayload`, calls `ser.Serialize(c.topic, &payload)` → returns `[]byte` with 5-byte Confluent prefix + JSON payload.
- `Decode(data []byte)` → calls `deser.DeserializeInto(c.topic, data, &payload)`, converts `ProductPayload` back to `*Product` including all nested unmarshal steps above.

### 6. Update `internal/product/processor.go`

Change `new(Codec)` to `NewCodec(string(inputTopic), srURL)` and `NewCodec(string(outputTopic), srURL)`. Add `srURL string` parameter to `RunProductProcessor`.

### 7. Update `cmd/app/main.go`

Read `SCHEMA_REGISTRY_URL` from env and pass it to `RunProductProcessor`.

### 8. Update `cmd/put-product/main.go`

Read `SCHEMA_REGISTRY_URL` from env (default `http://localhost:8081` for local use). Pass it to `NewCodec(string(util.ProductsRawTopic), srURL)`.

---

After all changes: rebuild the app image, restart the stack, re-register the connector via `kafka-connect-init`, then read logs from `kafka-connect` and `practicum-service-1` to verify no errors.
