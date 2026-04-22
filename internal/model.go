package internal

import "github.com/google/uuid"

type Message struct {
	UUID  uuid.UUID
	Value int
}

type SSLConfig struct {
	CALocation   string
	CertLocation string
	KeyLocation  string
}
