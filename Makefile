BIN_DIR := binaries
BINARIES := block-product put-product

.PHONY: all
all: bins up

.PHONY: bins
bins:
	@mkdir -p $(BIN_DIR)
	@for b in $(BINARIES); do \
		echo "Building $$b..."; \
		go build -o $(BIN_DIR)/$$b ./cmd/$$b; \
	done

.PHONY: up
up: build compose-up

.PHONY: down
down:
	docker compose down

.PHONY: build
build:
	docker compose build

.PHONY: compose-up
compose-up:
	docker compose up -d
