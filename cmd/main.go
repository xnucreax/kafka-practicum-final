package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"time"

	"kafka/internal"
)

func main() {
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP")
	topic := os.Getenv("KAFKA_TOPIC")
	caLocation := os.Getenv("KAFKA_SSL_CA_LOCATION")
	certLocation := os.Getenv("KAFKA_SSL_CERT_LOCATION")
	keyLocation := os.Getenv("KAFKA_SSL_KEY_LOCATION")

	if bootstrapServers == "" || topic == "" {
		log.Fatal("KAFKA_BOOTSTRAP and KAFKA_TOPIC env vars are required")
	}
	if caLocation == "" || certLocation == "" || keyLocation == "" {
		log.Fatal("KAFKA_SSL_CA_LOCATION, KAFKA_SSL_CERT_LOCATION and KAFKA_SSL_KEY_LOCATION env vars are required")
	}

	ssl := internal.SSLConfig{
		CALocation:   caLocation,
		CertLocation: certLocation,
		KeyLocation:  keyLocation,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producerId := rand.Int() % 1000
	if err := internal.RunProducer(ctx, internal.ProducerParams{
		BootstrapServers: bootstrapServers,
		Topic:            topic,
		Id:               producerId,
		SSL:              ssl,
		SendPeriod:       5 * time.Second,
	}); err != nil {
		log.Fatalf("failed to start producer: %v", err)
	}

	if err := internal.RunSingleMessageConsumer(ctx, internal.SingleMessageConsumerParams{
		BootstrapServers: bootstrapServers,
		GroupID:          "single-consumer-group",
		Topic:            topic,
		SSL:              ssl,
	}); err != nil {
		log.Fatalf("failed to start single consumer: %v", err)
	}

	// if err := internal.RunBatchMessageConsumer(ctx, internal.BatchMessageConsumerParams{
	// 	BootstrapServers: bootstrapServers,
	// 	GroupID:          "batch-consumer-group",
	// 	Topic:            topic,
	// 	SSL:              ssl,
	// }); err != nil {
	// 	log.Fatalf("failed to start batch consumer: %v", err)
	// }

	<-ctx.Done()
}
