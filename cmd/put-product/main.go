package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"practicum/internal/product"
	"practicum/internal/util"

	"github.com/IBM/sarama"
)

func openFile(path string) (*os.File, error) {
	if filepath.IsAbs(path) {
		return os.Open(path)
	}
	f, err := os.Open(path)
	if err == nil {
		return f, nil
	}
	abs, absErr := filepath.Abs(path)
	if absErr != nil {
		return nil, err
	}
	return os.Open(abs)
}

func buildSaramaConfig(caPath string) *sarama.Config {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("[put-product] cannot read CA cert: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		log.Fatalln("[put-product] failed to parse CA cert")
	}
	cfg := sarama.NewConfig()
	cfg.Net.TLS.Enable = true
	// EXTERNAL_SSL advertises 127.0.0.1 but cert has only DNS SANs, so pin ServerName
	cfg.Net.TLS.Config = &tls.Config{RootCAs: pool, ServerName: "kafka-1"}
	cfg.Producer.Return.Successes = true
	return cfg
}

func main() {
	path := flag.String("path", "", "path to product JSON file")
	ca := flag.String("ca", "infra/certificates/ca/certs/ca.crt", "path to CA certificate")
	flag.Parse()

	if *path == "" {
		log.Fatalln("--path is required")
	}

	f, err := openFile(*path)
	if err != nil {
		log.Fatalf("[put-product] cannot open file: %v", err)
	}
	defer f.Close()

	var p product.Product
	if err = json.NewDecoder(f).Decode(&p); err != nil {
		log.Fatalf("[put-product] cannot parse product: %v", err)
	}

	if p.ProductID == "" {
		log.Fatalln("[put-product] product_id is required")
	}

	value, err := new(product.Codec).Encode(&p)
	if err != nil {
		log.Fatalf("[put-product] encode error: %v", err)
	}

	producer, err := sarama.NewSyncProducer(util.Brokers, buildSaramaConfig(*ca))
	if err != nil {
		log.Fatalf("[put-product] cannot create producer: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: string(util.ProductsRawTopic),
		Key:   sarama.StringEncoder(p.ProductID),
		Value: sarama.ByteEncoder(value),
	}

	log.Printf("[put-product] emitting product: id=%s name=%s", p.ProductID, p.Name)
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("[put-product] error sending: %v", err)
	}
	log.Printf("[put-product] sent partition=%d offset=%d", partition, offset)
}
