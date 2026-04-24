package util

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"

	"github.com/IBM/sarama"
)

func NewTLSConfig(caPath string) *sarama.Config {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("[tls] cannot read CA cert: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		log.Fatalln("[tls] failed to parse CA cert")
	}
	cfg := sarama.NewConfig()
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = &tls.Config{RootCAs: pool}
	return cfg
}
