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
