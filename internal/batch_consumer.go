package internal

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"unsafe"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type BatchMessageConsumerParams struct {
	BootstrapServers string
	GroupID          string
	Topic            string
	SSL              SSLConfig
}

func RunBatchMessageConsumer(ctx context.Context, p BatchMessageConsumerParams) error {
	// magic number, gives >10 messages somehow
	minBytes := 50 * int(unsafe.Sizeof(Message{}))

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        p.BootstrapServers,
		"group.id":                 p.GroupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"fetch.min.bytes":          minBytes,
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

			batch, err := fetchBatchMessages(ctx, consumer)
			if err != nil {
				log.Printf("err fetching batch: %s\n", err.Error())
			}

			if len(batch) <= 0 {
				continue
			}

			var messages []Message
			for _, raw := range batch {
				var msg Message
				if err := json.Unmarshal(raw.Value, &msg); err != nil {
					log.Printf("failed to unmarshal message: %v", err)
					continue
				}
				messages = append(messages, msg)
			}

			log.Printf("batch size: %d, messages: %v", len(messages), messages)

			lastMessage := batch[len(batch)-1]
			if _, err := consumer.CommitMessage(lastMessage); err != nil {
				log.Printf("failed to commit offset: %v", err)
			}
		}
	}()

	return nil
}

func fetchBatchMessages(ctx context.Context, consumer *kafka.Consumer) ([]*kafka.Message, error) {
	// read messages from local buffer until it's empty
	var batch []*kafka.Message
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		kmsg, err := consumer.ReadMessage(time.Microsecond)
		if err != nil {
			if err.(kafka.Error).IsTimeout() == true {
				break
			}
			return nil, err
		}

		batch = append(batch, kmsg)
	}

	return batch, nil
}
