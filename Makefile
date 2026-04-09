.PHONY: help up down restart logs build test health topics flink-build flink-deploy flink-wait clean status validate

# Use Git Bash on Windows so Unix shell syntax works in recipes
# added for windows
ifeq ($(OS),Windows_NT)
    SHELL := C:/PROGRA~1/Git/bin/bash.exe
endif

FLINK_JAR_PATH ?= /opt/flink/usrlib/ingestion-hot-path-1.0.0.jar
FLINK_API_URL ?= http://localhost:8081
FLINK_DEPLOY_WAIT_SECONDS ?= 30

# added for windows
ifeq ($(OS),Windows_NT)
    PYTHON ?= python
else
    PYTHON ?= python3
endif

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

topics: ## Create Redpanda topics
	docker compose exec redpanda bash /etc/redpanda/scripts/create-topics.sh
# if command fails, replace the above with below for running locally
# 	bash ./redpanda/scripts/create-topics.sh

flink-build: ## Build Flink job JAR
	cd flink && mvn clean package -DskipTests

flink-deploy: flink-build ## Build and submit Flink job to cluster
	@RUNNING_COUNT=$$(curl -sf "$(FLINK_API_URL)/jobs/overview" | grep -o '"state":"RUNNING"' | wc -l | tr -d ' '); \
	if [ "$$RUNNING_COUNT" -gt 0 ]; then \
		echo "A Flink job is already RUNNING; skipping duplicate submission."; \
	else \
		echo "Submitting Flink job in detached mode..."; \
		docker compose exec -T flink-jobmanager flink run -d $(FLINK_JAR_PATH); \
	fi
	@$(MAKE) flink-wait

flink-wait: ## Wait until a Flink job reaches RUNNING state
	@echo "Waiting for Flink job to reach RUNNING state..."
	@for i in $$(seq 1 $(FLINK_DEPLOY_WAIT_SECONDS)); do \
		RUNNING_COUNT=$$(curl -sf "$(FLINK_API_URL)/jobs/overview" | grep -o '"state":"RUNNING"' | wc -l | tr -d ' '); \
		if [ "$$RUNNING_COUNT" -gt 0 ]; then \
			echo "Flink job is RUNNING."; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "No RUNNING Flink jobs found after $(FLINK_DEPLOY_WAIT_SECONDS)s." >&2; \
	curl -sf "$(FLINK_API_URL)/jobs/overview" || true; \
	exit 1

health: ## Run health checks on all services
	bash scripts/health-check.sh

test: ## Run integration tests
	cd tests && $(PYTHON) -m pytest -v

test-routing: ## Test hot/cold/DLQ routing only
	cd tests && $(PYTHON) -m pytest test_routing.py -v

test-e2e: ## Run full end-to-end pipeline test
	cd tests && $(PYTHON) -m pytest test_e2e_pipeline.py -v

generate-data: ## Generate sample telemetry data
	$(PYTHON) scripts/generate-test-data.py

clean: ## Remove volumes and data
	docker compose down -v
	rm -rf data/ volumes/