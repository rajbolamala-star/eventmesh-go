APP_NAME := eventmesh
GO_PACKAGES := ./...
POSTGRES_SERVICE := postgres
POSTGRES_USER := eventmesh
POSTGRES_DB := eventmesh

.PHONY: help tidy fmt test race build docker-build up down infra-up infra-down kafka-topics topics-create migrate-up migrate-down run-api run-worker integration-smoke load-smoke load-baseline load-stress load-soak load-spike perf-status

help:
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

tidy: ## Resolve Go module dependencies.
	go mod tidy

fmt: ## Format Go sources.
	go fmt $(GO_PACKAGES)

test: ## Run tests.
	go test $(GO_PACKAGES)

race: ## Run tests with the race detector.
	go test -race $(GO_PACKAGES)

build: ## Build service binaries.
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

docker-build: ## Build the API container image.
	docker build -t $(APP_NAME)-api:local .

up: ## Start local dependencies.
	docker compose up -d

infra-up: up ## Start local dependencies.

down: ## Stop local dependencies.
	docker compose down

infra-down: down ## Stop local dependencies.

kafka-topics: ## Create local Kafka topics.
	docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eventmesh.events --partitions 3 --replication-factor 1
	docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eventmesh.retry.1 --partitions 3 --replication-factor 1
	docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eventmesh.retry.2 --partitions 3 --replication-factor 1
	docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eventmesh.retry.3 --partitions 3 --replication-factor 1
	docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic eventmesh.dlq --partitions 3 --replication-factor 1

topics-create: kafka-topics ## Create local Kafka topics.

migrate-up: ## Apply database migrations using the Postgres container.
	@for file in migrations/*.up.sql; do docker compose exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 < "$$file"; done

migrate-down: ## Roll back database migrations using the Postgres container.
	@for file in $$(ls migrations/*.down.sql | sort -r); do docker compose exec -T $(POSTGRES_SERVICE) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 < "$$file"; done

run-api: ## Run the API locally.
	go run ./cmd/api

run-worker: ## Run the worker locally.
	go run ./cmd/worker

integration-smoke: ## Run the basic local integration smoke test.
	./scripts/smoke-test.sh

load-smoke: ## Run the k6 smoke ingestion load profile.
	LOAD_PROFILE=smoke k6 run loadtest/k6/events.js

load-baseline: ## Run the k6 baseline ingestion load profile.
	LOAD_PROFILE=baseline k6 run loadtest/k6/events.js

load-stress: ## Run the k6 stress ingestion load profile.
	LOAD_PROFILE=stress k6 run loadtest/k6/events.js

load-soak: ## Run the k6 soak ingestion load profile.
	LOAD_PROFILE=soak k6 run loadtest/k6/events.js

load-spike: ## Run the k6 spike ingestion load profile.
	LOAD_PROFILE=spike k6 run loadtest/k6/events.js

perf-status: ## Show local Kafka lag and recent event lifecycle state.
	docker compose exec -T kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group eventmesh-processors
	docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "SELECT status, COUNT(*) FROM events GROUP BY status ORDER BY status;"
	docker compose exec -T postgres psql -U eventmesh -d eventmesh -c "SELECT percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processed_at - created_at))) AS processing_latency_seconds FROM events WHERE status = 'PROCESSED' AND processed_at IS NOT NULL;"
