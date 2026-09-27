.PHONY: up down build test test-integration lint crash-test

up:
	docker compose up -d --wait

down:
	docker compose down

build:
	go build -o bin/ingest ./cmd/ingest
	go build -o bin/verify ./cmd/verify

test:
	go test ./...

test-integration: up
	go test -tags integration ./...

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"

crash-test: up build
	./scripts/chaos_ingest.sh
