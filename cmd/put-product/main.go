package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"practicum/internal/product"
	"practicum/internal/util"

	"github.com/lovoo/goka"
)

func openFile(path string) (*os.File, error) {
	if filepath.IsAbs(path) {
		return os.Open(path)
	}
	f, err := os.Open(path)
	if err == nil {
		return f, nil
	}
	abs, absErr := filepath.Abs(path)
	if absErr != nil {
		return nil, err
	}
	return os.Open(abs)
}

func main() {
	path := flag.String("path", "", "path to product JSON file")
	flag.Parse()

	if *path == "" {
		log.Fatalln("--path is required")
	}

	f, err := openFile(*path)
	if err != nil {
		log.Fatalf("[put-product] cannot open file: %v", err)
	}
	defer f.Close()

	var p product.Product
	if err = json.NewDecoder(f).Decode(&p); err != nil {
		log.Fatalf("[put-product] cannot parse product: %v", err)
	}

	if p.ProductID == "" {
		log.Fatalln("[put-product] product_id is required")
	}

	emitter, err := goka.NewEmitter(util.Brokers, util.ProductsRawTopic, new(product.Codec))
	if err != nil {
		log.Fatalf("[put-product] error creating emitter: %v", err)
	}
	defer emitter.Finish()

	log.Printf("[put-product] emitting product: id=%s name=%s", p.ProductID, p.Name)
	if err = emitter.EmitSync(p.ProductID, &p); err != nil {
		log.Fatalf("[put-product] error emitting: %v", err)
	}
}
