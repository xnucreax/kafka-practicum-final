package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"practicum/internal/blocker"
	"practicum/internal/product"
	"practicum/internal/util"

	"github.com/lovoo/goka"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	brokers := util.BrokersFromContainer

	srURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if srURL == "" {
		log.Fatalln("SCHEMA_REGISTRY_URL is required")
	}

	var opts []goka.ProcessorOption
	if caPath := os.Getenv("KAFKA_SSL_CA_LOCATION"); caPath != "" {
		cfg := util.NewTLSConfig(caPath)
		opts = append(opts,
			goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(cfg)),
			goka.WithConsumerSaramaBuilder(goka.SaramaConsumerBuilderWithConfig(cfg)),
			goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(cfg)),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithConfig(cfg, goka.NewTopicManagerConfig())),
		)
	}

	go blocker.RunBlockerProcessor(ctx, brokers, util.BlockerTopic, opts...)
	go product.RunProductProcessor(ctx, brokers, util.ProductsRawTopic, util.ProductsFilteredTopic, srURL, opts...)

	log.Println("processors started")
	<-ctx.Done()
	os.Exit(0)
}
