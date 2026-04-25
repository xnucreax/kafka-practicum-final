# Client Service — Implementation Plan

## Goal

Implement `cmd/client-service` — an HTTP API server with two endpoints:
- `POST /find-product` — searches PostgreSQL `products` table by name, writes query to Kafka
- `GET /recommendations` — stub returning placeholder JSON

---

## 1 — New file: `cmd/client-service/main.go`

### Dependencies
- HTTP server: `net/http` (stdlib)
- PostgreSQL: `github.com/jackc/pgx/v5` (`pgx.Connect`)
- Kafka producer: `github.com/confluentinc/confluent-kafka-go/kafka` (v1, already in go.mod)

### Environment variables
| Variable | Value in compose |
|----------|-----------------|
| `HTTP_ADDR` | `:8080` |
| `POSTGRES_DSN` | `postgres://postgres:postgres@postgres:5432/postgres` |
| `KAFKA_BOOTSTRAP` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| `KAFKA_SSL_CA_LOCATION` | `/etc/kafka/certs/ca.crt` |

### `POST /find-product`

Request body:
```json
{"name": "часы"}
```

Steps:
1. Decode `{"name": "..."}` from request body.
2. Query PostgreSQL:
   ```sql
   SELECT product_id, name, description, price, category, brand, stock,
          sku, tags, images, specifications, created_at, updated_at, index, store_id
   FROM products
   WHERE name ILIKE '%' || $1 || '%'
   ```
3. Return matching rows as JSON array.
4. Produce one message to Kafka topic `client-requests` (main cluster):
   - Key: empty
   - Value: Kafka Connect embedded-schema JSON (see §3 below)

### `GET /recommendations`

Stub — returns `{"message": "not implemented"}` with HTTP 200.

---

## 2 — Kafka topic `client-requests` on main cluster

Add `client-requests` to `TOPICS` array in `infra/kafka/kafka-init.sh`
(3 partitions, replication-factor 2).

Also add `client-requests` to `source->mirror.topics` in `infra/mirrormaker2/mm2.properties`
so analytics-service on the mirror cluster can consume `source.client-requests`.

Remove `client-requests` from `infra/kafka-mirror/init.sh` — it was created
there prematurely. With MM2 replication the topic will appear on the mirror cluster as
`source.client-requests`; a direct `client-requests` on the mirror cluster alongside it
would be a duplicate with no consumers.

Restart `mirrormaker2` after updating `mm2.properties` for the new topic to be picked up:
```
docker compose restart mirrormaker2
```

---

## 3 — Kafka message format for `client-requests`

The Debezium JDBC sink connector requires schema information.
Rather than integrating with Schema Registry, client-service embeds the schema
directly in the message body using Kafka Connect's own JSON envelope format:

```json
{
  "schema": {
    "type": "struct",
    "name": "ClientRequest",
    "optional": false,
    "fields": [
      {"field": "query", "type": "string", "optional": false}
    ]
  },
  "payload": {
    "query": "часы"
  }
}
```

The connector is configured with `value.converter.schemas.enable: true`
(schema comes from the message, not Schema Registry).

---

## 4 — Kafka Connect: new `client-requests` sink connector

Add a second `curl -X PUT` block to `infra/kafka-connect/init.sh`:

```json
{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "connection.url": "jdbc:postgresql://postgres:5432/postgres",
  "connection.username": "postgres",
  "connection.password": "postgres",
  "topics": "client-requests",
  "table.name.format": "client_requests",
  "insert.mode": "insert",
  "auto.create": "false",
  "db.timezone": "UTC",
  "tasks.max": "1",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true"
}
```

Connector name: `pg-client-requests-sink`.

---

## 5 — PostgreSQL: `client_requests` table

Add to `infra/postgres/postgres-init.sh`:

```sql
CREATE TABLE IF NOT EXISTS client_requests (
    query TEXT NOT NULL
);
```

---

## 6 — Dockerfile: add `client-service` stage

In the `builder` stage, add:
```dockerfile
RUN go build -o /app/bin/client-service ./cmd/client-service
```

Add a new final stage:
```dockerfile
FROM gcr.io/distroless/cc-debian12 AS client-service
COPY --from=builder /app/bin/client-service /client-service
ENTRYPOINT ["/client-service"]
```

---

## 7 — compose.yml: add `client-service` service

```yaml
client-service:
  build:
    context: .
    target: client-service
  ports:
    - "8080:8080"
  depends_on:
    kafka-1:
      condition: service_healthy
    kafka-init:
      condition: service_completed_successfully
    postgres:
      condition: service_healthy
    kafka-connect-init:
      condition: service_completed_successfully
  environment:
    - HTTP_ADDR=:8080
    - POSTGRES_DSN=postgres://postgres:postgres@postgres:5432/postgres
    - KAFKA_BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092
    - KAFKA_SSL_CA_LOCATION=/etc/kafka/certs/ca.crt
  volumes:
    - ./infra/certificates/ca/certs/ca.crt:/etc/kafka/certs/ca.crt:ro
```

---

## Files changed

| File | Change |
|------|--------|
| `cmd/client-service/main.go` | New — HTTP server with two handlers |
| `infra/kafka/kafka-init.sh` | Add `client-requests` to `TOPICS` |
| `infra/kafka-mirror/init.sh` | Remove `client-requests` (moves to main cluster, replicated by MM2) |
| `infra/mirrormaker2/mm2.properties` | Add `client-requests` to `source->mirror.topics` |
| `infra/kafka-connect/init.sh` | Add `pg-client-requests-sink` connector |
| `infra/postgres/postgres-init.sh` | Add `client_requests` table |
| `Dockerfile` | Add build + final stage for `client-service` |
| `compose.yml` | Add `client-service` service |
| `go.mod` / `go.sum` | Add `github.com/jackc/pgx/v5` |
