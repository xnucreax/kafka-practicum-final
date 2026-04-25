package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"github.com/jackc/pgx/v5"

	"practicum/internal/product"
	"practicum/internal/util"
)

type clientRequestPayload struct {
	Query string `json:"query"`
}

type findProductRequest struct {
	Name string `json:"name"`
}

type Recommendations struct {
	ProductIDs []string `json:"product_ids"`
}

var (
	db       *pgx.Conn
	producer *kafka.Producer

	latestRecs   Recommendations
	latestRecsMu sync.RWMutex
)

func main() {
	ctx := context.Background()

	var err error
	db, err = pgx.Connect(ctx, util.MustEnv("POSTGRES_DSN"))
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close(ctx)

	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": util.MustEnv("KAFKA_BOOTSTRAP"),
		"security.protocol": "SSL",
		"ssl.ca.location":   util.MustEnv("KAFKA_SSL_CA_LOCATION"),
	})
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	go consumeRecommendations(
		util.MustEnv("KAFKA_MIRROR_BOOTSTRAP"),
		util.MustEnv("KAFKA_SSL_CA_LOCATION"),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /find-product", handleFindProduct)
	mux.HandleFunc("GET /recommendations", handleRecommendations)

	addr := util.MustEnv("HTTP_ADDR")
	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func handleFindProduct(w http.ResponseWriter, r *http.Request) {
	var req findProductRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(r.Context(), `
		SELECT product_id, name, description, price, category, brand, stock,
		       sku, tags, images, specifications, created_at, updated_at, index, store_id
		FROM products
		WHERE name ILIKE '%' || $1 || '%'
	`, req.Name)
	if err != nil {
		log.Printf("query error: %v", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	products := []product.ProductPayload{}
	for rows.Next() {
		var p product.ProductPayload
		if err := rows.Scan(
			&p.ProductID, &p.Name, &p.Description, &p.Price, &p.Category,
			&p.Brand, &p.Stock, &p.SKU, &p.Tags, &p.Images,
			&p.Specifications, &p.CreatedAt, &p.UpdatedAt, &p.Index, &p.StoreID,
		); err != nil {
			log.Printf("scan error: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		log.Printf("rows error: %v", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	if err := produceClientRequest(req.Name); err != nil {
		log.Printf("produce client request: %v", err)
		http.Error(w, "kafka error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

func handleRecommendations(w http.ResponseWriter, r *http.Request) {
	latestRecsMu.RLock()
	ids := latestRecs.ProductIDs
	latestRecsMu.RUnlock()

	if len(ids) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]product.ProductPayload{})
		return
	}

	rows, err := db.Query(r.Context(), `
		SELECT product_id, name, description, price, category, brand, stock,
		       sku, tags, images, specifications, created_at, updated_at, index, store_id
		FROM products
		WHERE product_id = ANY($1)
	`, ids)
	if err != nil {
		log.Printf("recommendations query error: %v", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	products := []product.ProductPayload{}
	for rows.Next() {
		var p product.ProductPayload
		if err := rows.Scan(
			&p.ProductID, &p.Name, &p.Description, &p.Price, &p.Category,
			&p.Brand, &p.Stock, &p.SKU, &p.Tags, &p.Images,
			&p.Specifications, &p.CreatedAt, &p.UpdatedAt, &p.Index, &p.StoreID,
		); err != nil {
			log.Printf("recommendations scan error: %v", err)
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		log.Printf("recommendations rows error: %v", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

func consumeRecommendations(bootstrap, caLocation string) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           "client-service-recs",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
		"security.protocol":  "SSL",
		"ssl.ca.location":    caLocation,
	})
	if err != nil {
		log.Fatalf("create recs consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Subscribe("recommendations", nil); err != nil {
		log.Fatalf("subscribe recommendations: %v", err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("[recs] consumer error: %v", err)
			continue
		}
		var recs Recommendations
		if err := json.Unmarshal(msg.Value, &recs); err != nil {
			log.Printf("[recs] unmarshal: %v", err)
			continue
		}
		latestRecsMu.Lock()
		latestRecs = recs
		latestRecsMu.Unlock()
	}
}

func produceClientRequest(name string) error {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(util.MustEnv("SCHEMA_REGISTRY_URL")))
	if err != nil {
		log.Printf("create schema registry client: %v", err)
		return err
	}

	serializerConfig := jsonschema.NewSerializerConfig()
	serializerConfig.AutoRegisterSchemas = false
	serializerConfig.UseLatestVersion = true
	jsonSerializer, err := jsonschema.NewSerializer(client, serde.ValueSerde, serializerConfig)
	if err != nil {
		log.Printf("create json schema serializer: %v", err)
		return err
	}

	topic := "client-requests"
	value, err := jsonSerializer.Serialize(topic, &clientRequestPayload{Query: name})
	if err != nil {
		log.Printf("serialize client request: %v", err)
		return err
	}

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
	if err != nil {
		log.Printf("produce client request: %v", err)
		return err
	}
	return nil
}
