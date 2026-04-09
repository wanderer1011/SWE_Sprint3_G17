.PHONY: help up down restart logs build test health topics flink-build flink-deploy clean status validate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all Docker images (includes Flink JAR via multi-stage)
	docker compose build

up: build ## Start full pipeline (auto-creates topics, submits Flink job)
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

topics: ## Manually create Redpanda topics (auto-runs on 'make up')
	docker compose exec redpanda bash /etc/redpanda/scripts/create-topics.sh

flink-build: ## Build Flink job JAR locally (optional — Docker multi-stage handles this)
	cd flink && mvn clean package -DskipTests

flink-deploy: ## Manually submit Flink job (auto-runs on 'make up')
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
	python scripts/generate-test-data.py

clean: ## Remove volumes and data
	docker compose down -v
	rm -rf data/ volumes/
