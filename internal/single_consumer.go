package internal

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type SingleMessageConsumerParams struct {
	BootstrapServers string
	GroupID          string
	Topic            string
	SSL              SSLConfig
}

func RunSingleMessageConsumer(ctx context.Context, p SingleMessageConsumerParams) error {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        p.BootstrapServers,
		"group.id":                 p.GroupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       true,
		"security.protocol":        "ssl",
		"ssl.ca.location":          p.SSL.CALocation,
		"ssl.certificate.location": p.SSL.CertLocation,
		"ssl.key.location":         p.SSL.KeyLocation,
	})
	if err != nil {
		return err
	}

	if err := consumer.Subscribe(p.Topic, nil); err != nil {
		consumer.Close()
		return err
	}

	go func() {
		defer consumer.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			kmsg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).IsTimeout() != true {
					log.Printf("err fetching message: %s\n", err.Error())
				}
				continue
			}

			var msg Message
			if err := json.Unmarshal(kmsg.Value, &msg); err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				continue
			}

			log.Printf("received message: id %s, value %d", msg.UUID.String(), msg.Value)
		}
	}()

	return nil
}
