# Analytics Service — Implementation Plan

## Goal

Replicate the Spark `SparkAnalyticsJob` logic in Go inside `cmd/analytics-service/main.go`.
The service writes product and client-request data to a shared filesystem, then delegates
all analytical computation to Apache Spark via the `spark-connect-go` client library.
Results are published to a Kafka `recommendations` topic.

---

## What the Spark job does (mapped to the Go + Spark Connect approach)

| Step | Original Spark | Go + Spark Connect |
|------|----------------|--------------------|
| 1 | Read `/data/client-requests/*.json` from HDFS | `spark.Read().Format("json").Load("file:///data/client-requests/")` |
| 2 | Read `/data/allowed-products/*.json` from HDFS | `spark.Read().Format("json").Load("file:///data/allowed-products/")` |
| 3 | Filter `type = 'SEARCH_PRODUCT_REQUEST'` | SQL `WHERE type = 'SEARCH_PRODUCT_REQUEST'` |
| 4 | `groupBy("query").count()` → `event_count` | SQL `GROUP BY query` |
| 5 | `name_lower LIKE '%query_lower%'` join | SQL `LIKE concat('%', lower(query), '%')` |
| 6 | Random `viewed_count` (0–100), `purchased_count` (0–50) | SQL `CAST(rand()*100 AS INT)` |
| 7 | Write to `/analytics/recommendations/` | `df.Writer().Mode("overwrite").Format("json").Save(ctx, ...)` |
| 8 | Produce to Kafka `recommendations` | `df.Collect()` → iterate rows → confluent producer |

---

## Data structures

```go
// Client search event written to shared volume by the simulator goroutine
type ClientRequest struct {
    Type  string `json:"type"`
    Query string `json:"query"`
}

// Kafka value produced per recommendation (mirrors Spark's to_json(struct(...)))
type RecommendationValue struct {
    ProductID      string `json:"product_id"`
    PurchasedCount int    `json:"purchased_count"`
    ViewedCount    int    `json:"viewed_count"`
    EventCount     int    `json:"event_count"`
}
```

---

## 1 — Apache Spark in Docker Compose

Add a single `spark` service to `compose.yml` using the official `apache/spark:4.0.0` image.
Spark 4.0 ships with Spark Connect included — no extra JARs or `--packages` flag needed.

```yaml
spark:
  image: apache/spark:4.0.0
  ports:
    - "4040:4040"    # Spark UI
    - "15002:15002"  # Spark Connect gRPC endpoint
  volumes:
    - analytics-data:/data         # shared with analytics-service
    - analytics-results:/analytics # shared with analytics-service
  entrypoint: ["/bin/bash", "-c"]
  command: >
    /opt/spark/sbin/start-connect-server.sh --master local[*]
    --conf spark.connect.grpc.binding.port=15002
    && tail -f /opt/spark/logs/*.out
  healthcheck:
    test: ["CMD-SHELL", "nc -z localhost 15002 || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 10
    start_period: 30s
```

Also declare the named volumes at the top level of `compose.yml`:

```yaml
volumes:
  analytics-data:
  analytics-results:
```

### analytics-service in Docker Compose

The analytics-service binary needs to run inside the compose network so it can reach
`spark:15002`. Add a second build stage to the root `Dockerfile` (or a separate
`Dockerfile.analytics`) and a new compose service:

```yaml
analytics-service:
  build:
    context: .
    target: analytics     # second stage in Dockerfile
  depends_on:
    spark:
      condition: service_healthy
    kafka-mirror-1:
      condition: service_healthy
    kafka-1:
      condition: service_healthy
  environment:
    - SPARK_REMOTE=sc://spark:15002
    - KAFKA_MIRROR_BOOTSTRAP=kafka-mirror-1:9092,kafka-mirror-2:9092,kafka-mirror-3:9092
    - KAFKA_BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092
    - KAFKA_SSL_CA_LOCATION=/etc/kafka/certs/ca.crt
  volumes:
    - ./infra/certificates/ca/certs/ca.crt:/etc/kafka/certs/ca.crt:ro
    - analytics-data:/data
    - analytics-results:/analytics
```

### Dockerfile change

Add a second `FROM` stage that builds and runs `cmd/analytics-service`:

```dockerfile
FROM builder AS analytics
RUN go build -o /analytics-service ./cmd/analytics-service
ENTRYPOINT ["/analytics-service"]
```

---

## 2 — Add spark-connect-go dependency

```
go get github.com/apache/spark-connect-go/spark/sql
```

Import path used in code:

```go
import "github.com/apache/spark-connect-go/spark/sql"
```

Session builder (matches master-branch quick-start API):

```go
spark, err := sql.NewSessionBuilder().Remote("sc://spark:15002").Build(ctx)
defer spark.Stop()
```

---

## 3 — Changes to `cmd/analytics-service/main.go`

### 3a — Fix HDFS product write path

Current code writes to `/data/message_<uuid>` via the HDFS client at `localhost:9000`.
Replace with writes to the shared Docker volume path `/data/allowed-products/<product_id>.json`
using `os.WriteFile`. Drop the `colinmarc/hdfs` client entirely — Spark reads from
the same `/data` volume directly via `file:///data/...` paths.

Parse raw Kafka message bytes as `Product` JSON to extract `product_id` for the filename.

### 3b — Add client-request simulator goroutine

No `client-requests` Kafka topic exists, so simulate search events.
A goroutine fires every 5 s and writes one `ClientRequest` JSON file to
`/data/client-requests/request_<uuid>.json`.

```go
queries := []string{"часы", "телефон", "наушники", "ноутбук", "планшет"}
// pick random query, write {"type":"SEARCH_PRODUCT_REQUEST","query":"..."} to file
```

### 3c — Add Kafka producer for recommendations

```go
producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP"),  // kafka-1:9092,...
    "security.protocol": "SSL",
    "ssl.ca.location":   os.Getenv("KAFKA_SSL_CA_LOCATION"),
})
```

### 3d — Replace manual analytics logic with Spark Connect

Old approach (removed): manual file reads, Go loops, `strings.Contains` join.

New `runAnalyticsJob(ctx, spark, producer)`:

```go
// 1. Load datasets and register as temp views
products, _ := spark.Read().Format("json").Load("file:///data/allowed-products/")
products.CreateTempView(ctx, "products", true, false)

requests, _ := spark.Read().Format("json").Load("file:///data/client-requests/")
requests.CreateTempView(ctx, "client_requests", true, false)

// 2. Run analytics — mirrors SparkAnalyticsJob SQL logic exactly
recs, _ := spark.Sql(ctx, `
    SELECT
        q.query,
        p.product_id,
        CAST(rand() * 100 AS INT) AS viewed_count,
        CAST(rand() * 50  AS INT) AS purchased_count,
        q.event_count
    FROM (
        SELECT query, count(*) AS event_count
        FROM client_requests
        WHERE type = 'SEARCH_PRODUCT_REQUEST'
        GROUP BY query
    ) q
    JOIN products p
      ON lower(p.name) LIKE concat('%', lower(q.query), '%')
`)

// 3. Persist to shared volume (overwrite, same as Spark job)
recs.Writer().Mode("overwrite").Format("json").Save(ctx, "file:///analytics/recommendations/")

// 4. Collect rows and produce each to Kafka
rows, _ := recs.Collect()
for _, row := range rows {
    vals, _ := row.Values()
    // vals order: query, product_id, viewed_count, purchased_count, event_count
    key   := vals[0].(string)
    value := marshal RecommendationValue from vals[1..4]
    producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &recsTopic, Partition: kafka.PartitionAny},
        Key: []byte(key), Value: value,
    }, nil)
}
producer.Flush(5000)
```

### 3e — Run analytics job on a ticker

```go
ticker := time.NewTicker(30 * time.Second)
for range ticker.C {
    if err := runAnalyticsJob(ctx, spark, producer); err != nil {
        log.Printf("analytics job error: %v", err)
    }
}
```

---

## 4 — Separate infra directories for each Kafka cluster

Currently both clusters share `infra/kafka/` for config and init scripts.
Split into two dedicated directories:

```
infra/kafka/           # main cluster (kafka-1/2/3) — already exists
  client.properties    # bootstrap: kafka-1:9092,...
  kafka-init.sh        # creates topics for the main cluster
  product-schema.json

infra/kafka-mirror/    # mirror cluster (kafka-mirror-1/2/3) — new
  client.properties    # bootstrap: kafka-mirror-1:9092,...
  init.sh # creates mirror-side topics
```

### `infra/kafka-mirror/client.properties`

Copy of the main `client.properties` with broker addresses pointing at
`kafka-mirror-1:9092,kafka-mirror-2:9092,kafka-mirror-3:9092`.
SSL config (CA cert path, truststore type) stays the same.

### `infra/kafka-mirror/init.sh`

New init script that creates topics on the mirror cluster.
Mirrors the structure of `kafka-init.sh` but uses the mirror bootstrap
and creates only the topics the mirror cluster needs:

```bash
BOOTSTRAP="kafka-mirror-1:9092,kafka-mirror-2:9092,kafka-mirror-3:9092"
TOPICS=("client-requests" "recommendations")
```

`client-requests` and `recommendations` are produced/consumed on the mirror cluster
by the analytics-service, so they are created here rather than in the main init script.

### Add `kafka-mirror-init` service to `compose.yml`

```yaml
kafka-mirror-init:
  image: confluentinc/cp-kafka:latest
  depends_on:
    kafka-mirror-1:
      condition: service_healthy
    kafka-mirror-2:
      condition: service_healthy
    kafka-mirror-3:
      condition: service_healthy
  volumes:
    - ./infra/certificates/ca/certs/ca.crt:/etc/kafka/certs/ca.crt:ro
    - ./infra/kafka-mirror/client.properties:/etc/kafka/client.properties:ro
    - ./infra/kafka-mirror/init.sh:/init.sh:ro
  entrypoint: ["/bin/bash", "/init.sh"]
```

`analytics-service` in compose depends on `kafka-mirror-init` completing successfully
so topics exist before the consumer starts.

### Update `infra/kafka/kafka-init.sh`

Remove `client-requests` and `recommendations` from the main cluster init
(they now live in `init.sh`).

---

## 5 — Create Kafka topics in init scripts

Add `client-requests` and `recommendations` to `infra/kafka-mirror/init.sh`
(3 partitions, replication-factor 2).

`client-requests` allows the analytics-service to consume real search events
in addition to (or instead of) the simulator goroutine.

---

## Files changed

| File | Change |
|------|--------|
| `cmd/analytics-service/main.go` | Full rewrite: drop HDFS client, add Spark Connect session, add simulator goroutine, add Kafka producer, replace manual analytics with `runAnalyticsJob` using Spark SQL, add ticker |
| `Dockerfile` | Add `analytics` build stage for `cmd/analytics-service` |
| `compose.yml` | Add `spark` service, add `analytics-service` service, declare `analytics-data` and `analytics-results` named volumes |
| `infra/kafka/kafka-init.sh` | Remove `client-requests` / `recommendations` (moved to mirror init) |
| `infra/kafka-mirror/client.properties` | New — mirror cluster broker addresses, same SSL config |
| `infra/kafka-mirror/init.sh` | New — creates `client-requests` and `recommendations` on mirror cluster |
| `compose.yml` | Add `kafka-mirror-init` service; update `analytics-service` depends_on |
| `go.mod` / `go.sum` | Add `github.com/apache/spark-connect-go` |

Removed dependency: `github.com/colinmarc/hdfs/v2` (replaced by shared Docker volume + `os.WriteFile`).
