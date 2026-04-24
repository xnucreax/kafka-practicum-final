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
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	brokers := util.BrokersFromContainer

	go blocker.RunBlockerProcessor(ctx, brokers, util.BlockerTopic)
	go product.RunProductProcessor(ctx, brokers, util.ProductsRawTopic, util.ProductsFilteredTopic)

	log.Println("processors started")
	<-ctx.Done()
	os.Exit(0)
}
