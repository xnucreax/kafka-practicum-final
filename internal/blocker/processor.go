package blocker

import (
	"context"
	"log"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	BlockerGroup goka.Group = "blocker"
)

type ValueCodec struct {
	codec.Int64 // 0 means not blocked, 1 means blocked
}

func addBlockedProduct(ctx goka.Context, msg interface{}) {
	isBlocked := msg.(int64)
	log.Printf("[blocker] key=%s: input isBlocked=%q", ctx.Key(), isBlocked)

	if existing := ctx.Value(); existing != nil {
		log.Printf("[blocker] key=%s: existing context value=%q", ctx.Key(), existing.(int64))
	}

	log.Printf("[blocker] key=%s: saving isBlocked=%q", ctx.Key(), isBlocked)
	ctx.SetValue(isBlocked)
}

func RunBlockerProcessor(ctx context.Context, broker []string, inputStream goka.Stream) {
	g := goka.DefineGroup(
		BlockerGroup,
		goka.Input(
			inputStream,
			new(ValueCodec),
			addBlockedProduct,
		),
		goka.Persist(
			new(ValueCodec),
		),
	)

	p, err := goka.NewProcessor(broker, g)
	if err != nil {
		log.Fatalf("[blocker] error creating processor: %v", err)
	}

	err = p.Run(ctx)
	if err != nil {
		log.Fatalf("[blocker] error running processor: %v", err)
	}
}
