package util

import "github.com/lovoo/goka"

var (
	Brokers = []string{"localhost:9094", "localhost:9095", "localhost:9096"}

	BrokersFromContainer = []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}

	ProductsRawTopic      goka.Stream = "shop-products-raw"
	ProductsFilteredTopic goka.Stream = "shop-products-filtered"

	BlockerTopic goka.Stream = "shop-blocked"
	BlockerTable goka.Table  = "shop-blocked-table"
	// ProductsTable goka.Table = "products-table"
)
