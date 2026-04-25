package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/spark-connect-go/v34/client/sql"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	productsDir = "/data/allowed-products"
	requestsDir = "/data/client-requests"
	resultsDir  = "/analytics/recommendations"
	recsTopic   = "recommendations"
)

// sparkSession is a local interface so we can pass the session to helper functions
// without naming the unexported sql.sparkSession type from the library.
type sparkSession interface {
	Read() sql.DataFrameReader
	Sql(query string) (sql.DataFrame, error)
}

type RecommendationValue struct {
	ProductID      string `json:"product_id"`
	PurchasedCount int    `json:"purchased_count"`
	ViewedCount    int    `json:"viewed_count"`
	EventCount     int    `json:"event_count"`
}

func main() {
	for _, dir := range []string{productsDir, requestsDir, resultsDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	caLocation := mustEnv("KAFKA_SSL_CA_LOCATION")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  mustEnv("KAFKA_MIRROR_BOOTSTRAP"),
		"group.id":           "analytics-consumer-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
		"session.timeout.ms": 6000,
		"security.protocol":  "SSL",
		"ssl.ca.location":    caLocation,
	})
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	if err := consumer.Subscribe("source.shop-products-filtered", nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": mustEnv("KAFKA_BOOTSTRAP"),
		"security.protocol": "SSL",
		"ssl.ca.location":   caLocation,
	})
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	spark, err := sql.SparkSession.Builder.Remote(mustEnv("SPARK_REMOTE")).Build()
	if err != nil {
		log.Fatalf("connect to Spark: %v", err)
	}
	defer spark.Stop()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := runAnalyticsJob(spark, producer); err != nil {
				log.Printf("[analytics] job error: %v", err)
			}
		}
	}()

	// Consume products from the mirror cluster and persist to shared volume.
	// msg.Value is Confluent wire format: 1 magic byte + 4 schema-ID bytes + JSON payload.
	for {
		msg, err := consumer.ReadMessage(100 * time.Millisecond)
		if err == nil {
			writeProduct(msg.Value)
		} else {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsFatal() {
				log.Fatalf("fatal kafka error: %v", kafkaErr)
			}
		}
	}
}

func writeProduct(raw []byte) {
	// Strip the 5-byte Confluent wire-format prefix before writing clean JSON.
	if len(raw) < 6 {
		return
	}
	jsonBytes := raw[5:]

	var p struct {
		ProductID string `json:"product_id"`
	}
	if err := json.Unmarshal(jsonBytes, &p); err != nil || p.ProductID == "" {
		return
	}

	path := fmt.Sprintf("%s/%s.json", productsDir, p.ProductID)
	if err := os.WriteFile(path, jsonBytes, 0644); err != nil {
		log.Printf("write product %s: %v", p.ProductID, err)
	}
}

func runAnalyticsJob(spark sparkSession, producer *kafka.Producer) error {
	products, err := spark.Read().Format("json").Load("file://" + productsDir)
	if err != nil {
		return fmt.Errorf("load products: %w", err)
	}
	if err := products.CreateTempView("products", true, false); err != nil {
		return fmt.Errorf("create products view: %w", err)
	}

	requests, err := spark.Read().Format("json").Load("file://" + requestsDir)
	if err != nil {
		return fmt.Errorf("load requests: %w", err)
	}
	if err := requests.CreateTempView("client_requests", true, false); err != nil {
		return fmt.Errorf("create requests view: %w", err)
	}

	recs, err := spark.Sql(`
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
	if err != nil {
		return fmt.Errorf("analytics sql: %w", err)
	}

	if err := recs.Write().Mode("overwrite").Format("json").Save("file://" + resultsDir); err != nil {
		return fmt.Errorf("save recommendations: %w", err)
	}

	rows, err := recs.Collect()
	if err != nil {
		return fmt.Errorf("collect rows: %w", err)
	}

	topic := recsTopic
	count := 0
	for _, row := range rows {
		vals, err := row.Values()
		if err != nil || len(vals) < 5 {
			continue
		}
		query, _ := vals[0].(string)
		productID, _ := vals[1].(string)

		value := RecommendationValue{
			ProductID:      productID,
			ViewedCount:    anyToInt(vals[2]),
			PurchasedCount: anyToInt(vals[3]),
			EventCount:     anyToInt(vals[4]),
		}
		valueBytes, _ := json.Marshal(value)

		_ = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(query),
			Value:          valueBytes,
		}, nil)
		count++
	}
	producer.Flush(5000)
	log.Printf("[analytics] produced %d recommendations", count)
	return nil
}

func anyToInt(v any) int {
	switch n := v.(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	}
	return 0
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
