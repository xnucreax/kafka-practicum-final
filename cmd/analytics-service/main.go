package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/apache/spark-connect-go/v34/client/sql"
	"github.com/colinmarc/hdfs/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	"practicum/internal/util"
)

const (
	hdfsNamenode = "hadoop-namenode:9000"
	productsDir  = "/data/allowed-products"
	requestsDir  = "/data/client-requests"
	resultsDir   = "/analytics/recommendations"
	recsTopic    = "recommendations"

	topicProducts = "source.shop-products-filtered"
	topicRequests = "source.client-requests"
)

// sparkSession is a local interface to avoid naming the unexported sql.sparkSession type.
type sparkSession interface {
	Read() sql.DataFrameReader
	Sql(query string) (sql.DataFrame, error)
}

type Recommendations struct {
	ProductIDs []string `json:"product_ids"`
}

func main() {
	ctx := context.Background()

	caLocation := util.MustEnv("KAFKA_SSL_CA_LOCATION")
	mirrorBootstrap := util.MustEnv("KAFKA_MIRROR_BOOTSTRAP")
	sparkRemote := util.MustEnv("SPARK_REMOTE")

	dialFunc := (&net.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext

	hdfsClient, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses:           []string{hdfsNamenode},
		User:                "root",
		NamenodeDialFunc:    dialFunc,
		DatanodeDialFunc:    dialFunc,
		UseDatanodeHostname: true,
	})
	if err != nil {
		log.Fatalf("create hdfs client: %v", err)
	}
	defer hdfsClient.Close()

	for _, dir := range []string{productsDir, requestsDir, resultsDir} {
		if err := hdfsClient.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("hdfs mkdir %s: %v", dir, err)
		}
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  mirrorBootstrap,
		"group.id":           "analytics-consumer-group",
		"enable.auto.commit": false,
		"session.timeout.ms": 6000,
		"security.protocol":  "SSL",
		"ssl.ca.location":    caLocation,
	})
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	if err := consumer.SubscribeTopics([]string{
		topicProducts,
		topicRequests,
	}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": mirrorBootstrap,
		"security.protocol": "SSL",
		"ssl.ca.location":   caLocation,
	})
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	spark, err := sql.SparkSession.Builder.Remote(sparkRemote).Build()
	if err != nil {
		log.Fatalf("connect to Spark: %v", err)
	}
	defer spark.Stop()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := runAnalyticsJob(spark, hdfsClient, producer); err != nil {
					log.Printf("[analytics] job error: %v", err)
				}
			}
		}
	}()

	for {
		msg, err := consumer.ReadMessage(100 * time.Millisecond)

		if err == nil {
			var writeErr error
			switch *msg.TopicPartition.Topic {
			case topicProducts:
				writeErr = writeProduct(hdfsClient, msg.Value)
			case topicRequests:
				writeErr = writeClientRequest(hdfsClient, msg.Value)
			}
			if writeErr != nil {
				log.Printf("write message: %v", writeErr)
			} else if _, err := consumer.CommitMessage(msg); err != nil {
				log.Printf("commit offset: %v", err)
			}
		} else {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.IsFatal() {
				log.Fatalf("fatal kafka error: %v", kafkaErr)
			}
		}
	}
}

// writeProduct strips the 5-byte Confluent wire-format prefix and writes the JSON to HDFS.
// Overwrites if the file already exists (products are keyed by product_id).
func writeProduct(client *hdfs.Client, raw []byte) error {
	if len(raw) < 6 {
		return fmt.Errorf("message too short")
	}
	jsonBytes := raw[5:]

	var p struct {
		ProductID string `json:"product_id"`
	}
	if err := json.Unmarshal(jsonBytes, &p); err != nil {
		return fmt.Errorf("unmarshal product: %w", err)
	}
	if p.ProductID == "" {
		return fmt.Errorf("empty product_id")
	}

	path := fmt.Sprintf("%s/%s.json", productsDir, p.ProductID)
	// _ = client.Remove(path)
	return writeHDFS(client, path, jsonBytes)
}

func writeClientRequest(client *hdfs.Client, raw []byte) error {
	if len(raw) < 6 {
		return fmt.Errorf("message too short")
	}
	jsonBytes := raw[5:]

	var req struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		return fmt.Errorf("unmarshal client request: %w", err)
	}
	if req.Query == "" {
		return fmt.Errorf("empty query")
	}

	path := fmt.Sprintf("%s/%s.json", requestsDir, uuid.New().String())
	return writeHDFS(client, path, jsonBytes)
}

func writeHDFS(client *hdfs.Client, path string, data []byte) error {
	writer, err := client.Create(path)
	if err != nil {
		return fmt.Errorf("hdfs create %s: %w", path, err)
	}
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("hdfs write %s: %w", path, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("hdfs close %s: %w", path, err)
	}
	return nil
}

func isNoDataErr(err error) bool {
	return strings.Contains(err.Error(), "UNABLE_TO_INFER_SCHEMA")
}

func hdfsHasFiles(client *hdfs.Client, dir string) bool {
	entries, err := client.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			return true
		}
	}
	return false
}

func runAnalyticsJob(spark sparkSession, hdfsClient *hdfs.Client, producer *kafka.Producer) error {
	if !hdfsHasFiles(hdfsClient, productsDir) {
		log.Printf("[analytics] skipping job: no products in hdfs yet")
		return nil
	}
	if !hdfsHasFiles(hdfsClient, requestsDir) {
		log.Printf("[analytics] skipping job: no client requests in hdfs yet")
		return nil
	}

	hdfsBase := "hdfs://" + hdfsNamenode

	products, err := spark.Read().Format("json").Load(hdfsBase + productsDir)
	if err != nil {
		return fmt.Errorf("load products: %w", err)
	}
	if err := products.CreateTempView("products", true, false); err != nil {
		if isNoDataErr(err) {
			log.Printf("[analytics] skipping job: no products in hdfs yet")
			return nil
		}
		return fmt.Errorf("create products view: %w", err)
	}

	requests, err := spark.Read().Format("json").Load(hdfsBase + requestsDir)
	if err != nil {
		return fmt.Errorf("load requests: %w", err)
	}
	if err := requests.CreateTempView("client_requests", true, false); err != nil {
		if isNoDataErr(err) {
			log.Printf("[analytics] skipping job: no client requests in hdfs yet")
			return nil
		}
		return fmt.Errorf("create requests view: %w", err)
	}

	recs, err := spark.Sql(`
		SELECT DISTINCT p.product_id
		FROM client_requests cr
		JOIN products p ON lower(p.name) LIKE concat('%', lower(cr.query), '%')
	`)
	if err != nil {
		return fmt.Errorf("analytics sql: %w", err)
	}

	// Collect once — avoids executing the Spark query a second time for Write().
	rows, err := recs.Collect()
	if err != nil {
		return fmt.Errorf("collect rows: %w", err)
	}

	result := Recommendations{
		ProductIDs: make([]string, len(rows)),
	}
	for i, row := range rows {
		vals, err := row.Values()
		if err != nil || len(vals) < 1 {
			continue
		}
		productID, _ := vals[0].(string)
		result.ProductIDs[i] = productID
	}

	resultsFile := resultsDir + "/results.json"
	_ = hdfsClient.Remove(resultsFile)
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	if err := writeHDFS(hdfsClient, resultsFile, data); err != nil {
		log.Printf("[analytics] write results to hdfs: %v", err)
	}

	topic := recsTopic
	_ = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: data,
	}, nil)

	producer.Flush(5000)
	log.Printf("[analytics] produced recommendations: %v", result.ProductIDs)
	return nil
}
