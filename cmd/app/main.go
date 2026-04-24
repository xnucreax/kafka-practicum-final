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
	go product.RunProductProcessor(ctx, brokers, util.ProductsRawTopic, util.ProductsFilteredTopic, opts...)

	log.Println("processors started")
	<-ctx.Done()
	os.Exit(0)
}
