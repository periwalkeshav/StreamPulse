.DEFAULT_GOAL := help
COMPOSE := docker compose
TEST_COMPOSE := docker compose -f docker-compose.test.yml
PSQL := $(COMPOSE) exec -T postgres psql -U streampulse -d streampulse

.PHONY: help up down build logs ps clean psql topics lag test test-unit test-integration \
        lint format loadtest smoke dashboard-export producer-burst demo

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the producer and Spark images
	$(COMPOSE) build

up: ## Start the entire stack
	$(COMPOSE) up -d --build
	@echo
	@echo "  Kafka UI  http://localhost:8080"
	@echo "  Grafana   http://localhost:3000   (admin / admin)"
	@echo "  Airflow   http://localhost:8081   (admin / admin)"
	@echo "  Spark UI  http://localhost:4040"

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

clean: ## Stop the stack and delete all data
	$(COMPOSE) down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail the pipeline logs
	$(COMPOSE) logs -f producer spark-consumer dlq-monitor

psql: ## Open a psql shell
	$(COMPOSE) exec postgres psql -U streampulse -d streampulse

topics: ## List Kafka topics and their configuration
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --describe

lag: ## Show consumer group lag
	@./scripts/kafka_lag.sh

smoke: ## Print live pipeline counters
	@$(PSQL) -c "SELECT * FROM v_pipeline_health;"
	@$(PSQL) -c "SELECT event_type, sum(total_events) AS events, round(avg(p95_latency_ms), 1) AS p95_ms FROM aggregated_metrics GROUP BY 1 ORDER BY 2 DESC;"
	@$(PSQL) -c "SELECT error_type, count(*) FROM dlq_events GROUP BY 1 ORDER BY 2 DESC;"

test: test-unit ## Alias for test-unit

test-unit: ## Run unit tests inside the Spark image (Java included)
	$(COMPOSE) run --rm --no-deps spark-consumer tests tests/test_producer.py tests/test_transformations.py -v

test-integration: ## Run integration tests against the running stack
	$(COMPOSE) run --rm -e RUN_INTEGRATION=1 spark-consumer tests tests/test_integration.py -v

ci-test: ## Reproduce the CI integration job locally
	$(TEST_COMPOSE) up -d --build
	sleep 90
	$(TEST_COMPOSE) run --rm -e RUN_INTEGRATION=1 spark-consumer tests tests/test_integration.py -v
	$(TEST_COMPOSE) down -v

lint: ## flake8 + black --check
	$(COMPOSE) run --rm --no-deps --entrypoint sh spark-consumer -c \
		"pip install -q flake8==7.1.0 black==24.4.2 && flake8 producer consumer loadtest sql tests && black --check --line-length 110 producer consumer loadtest sql tests"

loadtest: ## Drive 10K events/sec with Locust (headless, 3 minutes)
	$(COMPOSE) --profile loadtest run --rm locust \
		locust -f loadtest/locustfile.py --headless -u 20 -r 5 -t 3m

producer-burst: ## One-off 30s burst at 5,000 events/sec
	$(COMPOSE) run --rm -e EVENTS_PER_SECOND=5000 -e DURATION_SECONDS=30 producer

demo: ## Bring everything up and wait until data is flowing
	@./scripts/demo.sh
