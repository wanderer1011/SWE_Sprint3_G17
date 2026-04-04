.PHONY: help up down restart logs build test health topics flink-build flink-deploy clean status validate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all Docker images
	docker compose build

up: build ## Start the full ingestion pipeline (builds first)
	docker compose up -d --wait

down: ## Stop the full ingestion pipeline
	docker compose down

restart: ## Restart all services
	docker compose down && docker compose up -d --wait

logs: ## Tail logs from all services
	docker compose logs -f

status: ## Show service health status
	docker compose ps

validate: ## Validate docker-compose.yml syntax
	docker compose config --quiet

topics: ## Create Redpanda topics, commented below since it doesn't work with the config, hence local config is intended
# 	docker compose exec redpanda bash /etc/redpanda/scripts/create-topics.sh
	bash ./redpanda/scripts/create-topics.sh

flink-build: ## Build Flink job JAR
	cd flink && mvn clean package -DskipTests

flink-deploy: flink-build ## Build and submit Flink job to cluster
	docker compose exec flink-jobmanager flink run \
		/opt/flink/usrlib/ingestion-hot-path-1.0.0.jar

health: ## Run health checks on all services
	bash scripts/health-check.sh

test: ## Run integration tests
	cd tests && python -m pytest -v

test-routing: ## Test hot/cold/DLQ routing only
	cd tests && python -m pytest test_routing.py -v

test-e2e: ## Run full end-to-end pipeline test
	cd tests && python -m pytest test_e2e_pipeline.py -v

generate-data: ## Generate sample telemetry data
	python3 scripts/generate-test-data.py

clean: ## Remove volumes and data
	docker compose down -v
	rm -rf data/ volumes/
