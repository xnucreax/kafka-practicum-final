package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type ProducerParams struct {
	BootstrapServers string
	Topic            string
	Id               int
	SSL              SSLConfig
	SendPeriod       time.Duration
}

func RunProducer(ctx context.Context, p ProducerParams) error {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        p.BootstrapServers,
		"acks":                     "all",
		"retries":                  3,
		"security.protocol":        "ssl",
		"ssl.ca.location":          p.SSL.CALocation,
		"ssl.certificate.location": p.SSL.CertLocation,
		"ssl.key.location":         p.SSL.KeyLocation,
	})
	if err != nil {
		return err
	}

	go func() {
		defer producer.Close()

		tmr := time.NewTicker(p.SendPeriod)
		defer tmr.Stop()

		for {
			deliveryChan := make(chan kafka.Event)

			select {
			case <-ctx.Done():
				return
			case <-tmr.C:
			}

			id, _ := uuid.NewRandom()
			msg := Message{
				UUID:  id,
				Value: p.Id,
			}
			data, _ := json.Marshal(msg)

			kmsg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &p.Topic,
					Partition: kafka.PartitionAny,
				},
				Value: data,
			}

			err = producer.Produce(kmsg, deliveryChan)
			if err != nil {
				log.Printf("failed to produce message: %v", err)
			} else {
				// log.Printf("produced message: id %x, value %d", msg.UUID, msg.Value)
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				fmt.Printf("Ошибка доставки сообщения: %v\n", m.TopicPartition.Error)
			} else {
				// fmt.Printf("Сообщение отправлено в топик %s [%d] офсет %v\n",
				// 	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

			for producer.Flush(10000) > 0 {
				fmt.Print("Still waiting to flush outstanding messages")
			}

			log.Printf("produced message: id %s, value %d", msg.UUID.String(), msg.Value)
		}
	}()

	return nil
}
