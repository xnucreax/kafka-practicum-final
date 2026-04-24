package product

import (
	"context"
	"log"

	"practicum/internal/blocker"

	"github.com/lovoo/goka"
)

var (
	ProductGroup goka.Group = "shop-products"
)

func processProductFunc(outputTopic goka.Stream) func(ctx goka.Context, input any) {
	return func(ctx goka.Context, input any) {
		product, ok := input.(*Product)
		if !ok {
			log.Printf("[product] key=%s: received non-message input, skipping", ctx.Key())
			return
		}

		log.Printf("[product] key=%s: input product %q", ctx.Key(), product.ProductID)

		tags := product.Tags
		for _, tag := range tags {
			isBlockedInt := ctx.Lookup(goka.GroupTable(blocker.BlockerGroup), tag)
			isBlocked, ok := isBlockedInt.(int64)
			if !ok {
				log.Printf("[product] key=%s: tag %q is not blocked", ctx.Key(), tag)
				continue
			}
			if isBlocked == 1 {
				log.Printf("[product] key=%s: tag %q is blocked, dropping product", ctx.Key(), tag)
				return
			}
		}

		log.Printf("[product] key=%s: emit output product, name=%q, tags=%v", ctx.Key(), product.Name, product.Tags)
		ctx.Emit(outputTopic, ctx.Key(), product)
	}
}

func RunProductProcessor(ctx context.Context, brokers []string, inputTopic goka.Stream, outputTopic goka.Stream, opts ...goka.ProcessorOption) {
	g := goka.DefineGroup(
		ProductGroup,
		goka.Input(
			inputTopic,
			new(Codec),
			processProductFunc(outputTopic),
		),
		goka.Output(
			outputTopic,
			new(Codec),
		),
		goka.Lookup(
			goka.GroupTable(blocker.BlockerGroup),
			new(blocker.ValueCodec),
		),
	)

	p, err := goka.NewProcessor(brokers, g, opts...)
	if err != nil {
		log.Fatalf("[product] error creating processor: %v", err)
	}

	err = p.Run(ctx)
	if err != nil {
		log.Fatalf("[product] error running processor: %v", err)
	}
}
