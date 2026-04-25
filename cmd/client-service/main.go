package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
)

type findProductRequest struct {
	Name string `json:"name"`
}

type product struct {
	ProductID      string `json:"product_id"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	Price          string `json:"price"`
	Category       string `json:"category"`
	Brand          string `json:"brand"`
	Stock          string `json:"stock"`
	SKU            string `json:"sku"`
	Tags           string `json:"tags"`
	Images         string `json:"images"`
	Specifications string `json:"specifications"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
	Index          string `json:"index"`
	StoreID        string `json:"store_id"`
}

// clientRequestMessage is the Kafka Connect embedded-schema envelope.
// JsonConverter with schemas.enable=true reads this on the sink side.
type clientRequestMessage struct {
	Schema  clientRequestSchema  `json:"schema"`
	Payload clientRequestPayload `json:"payload"`
}

type clientRequestSchema struct {
	Type     string                  `json:"type"`
	Name     string                  `json:"name"`
	Optional bool                    `json:"optional"`
	Fields   []clientRequestField    `json:"fields"`
}

type clientRequestField struct {
	Field    string `json:"field"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

type clientRequestPayload struct {
	Query string `json:"query"`
}

var (
	db       *pgx.Conn
	producer *kafka.Producer
)

func main() {
	ctx := context.Background()

	var err error
	db, err = pgx.Connect(ctx, mustEnv("POSTGRES_DSN"))
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer db.Close(ctx)

	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": mustEnv("KAFKA_BOOTSTRAP"),
		"security.protocol": "SSL",
		"ssl.ca.location":   mustEnv("KAFKA_SSL_CA_LOCATION"),
	})
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /find-product", handleFindProduct)
	mux.HandleFunc("GET /recommendations", handleRecommendations)

	addr := mustEnv("HTTP_ADDR")
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

	products := []product{}
	for rows.Next() {
		var p product
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

	produceClientRequest(req.Name)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(products)
}

func handleRecommendations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "not implemented"})
}

func produceClientRequest(query string) {
	msg := clientRequestMessage{
		Schema: clientRequestSchema{
			Type:     "struct",
			Name:     "ClientRequest",
			Optional: false,
			Fields: []clientRequestField{
				{Field: "query", Type: "string", Optional: false},
			},
		},
		Payload: clientRequestPayload{Query: query},
	}
	value, _ := json.Marshal(msg)

	topic := "client-requests"
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
	if err != nil {
		log.Printf("produce client-request: %v", err)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env %s is required", key)
	}
	return v
}
