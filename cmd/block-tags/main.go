package main

import (
	"flag"
	"log"

	"practicum/internal/blocker"
	"practicum/internal/util"

	"github.com/lovoo/goka"
)

func main() {
	tag := flag.String("tag", "", "tag to block or unblock")
	blocked := flag.Bool("blocked", true, "true to block, false to unblock")
	flag.Parse()

	if *tag == "" {
		log.Fatalln("--tag is required")
	}

	emitter, err := goka.NewEmitter(util.Brokers, util.BlockerTopic, new(blocker.ValueCodec))
	if err != nil {
		log.Fatalf("[block-tags] error creating emitter: %v", err)
	}
	defer emitter.Finish()

	var value int64
	if *blocked {
		value = 1
	}

	log.Printf("[block-tags] tag=%s blocked=%v", *tag, *blocked)
	if err = emitter.EmitSync(*tag, value); err != nil {
		log.Fatalf("[block-tags] error emitting: %v", err)
	}
}
