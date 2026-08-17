.PHONY: help install up down test lint fmt ingest ingest-full describe smoke clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install the package plus dev dependencies
	pip install -e ".[dev]"

up: ## Start LocalStack + Postgres
	docker compose -f docker/docker-compose.yml up -d
	@echo "waiting for localstack..."
	@until curl -sf http://localhost:4566/_localstack/health > /dev/null; do sleep 2; done
	@echo "waiting for postgres..."
	@until docker exec denver311-postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done
	@echo "ready"

down: ## Stop local stack
	docker compose -f docker/docker-compose.yml down

test: ## Run the test suite
	pytest --cov=denver311 --cov-report=term-missing

lint: ## Lint
	ruff check src tests

fmt: ## Auto-fix and format
	ruff check --fix src tests && ruff format src tests

describe: ## Print the live source layer schema (requires internet)
	python -m denver311.ingestion.run_ingest --describe

smoke: ## Hit the live API without writing anything
	python -m denver311.ingestion.run_ingest --dry-run

ingest: ## Incremental ingestion run
	python -m denver311.ingestion.run_ingest

ingest-full: ## Full refresh, ignoring the watermark
	python -m denver311.ingestion.run_ingest --full

transform: ## Clean raw data with Spark, write processed/ Parquet + a readable CSV preview
	python -m spark_jobs.run_transform

preview: ## Open the readable CSV preview from the last transform run
	open output/service_requests_preview.csv 2>/dev/null || cat output/service_requests_preview.csv

load-warehouse: ## Load cleaned parquet into the Postgres star schema
	python -m warehouse.load_warehouse

dashboard: ## Launch the Streamlit dashboard
	streamlit run dashboard/app.py

airflow-up: ## Start Airflow (webserver + scheduler) alongside the base stack
	docker compose -f docker/docker-compose.yml -f docker/docker-compose.airflow.yml up -d

airflow-down: ## Stop Airflow
	docker compose -f docker/docker-compose.yml -f docker/docker-compose.airflow.yml down

pipeline: ## Run the full pipeline end to end: ingest -> transform -> load
	$(MAKE) ingest
	$(MAKE) transform
	$(MAKE) load-warehouse

ls-raw: ## List what has landed in the local bucket
	aws --endpoint-url=http://localhost:4566 s3 ls s3://denver311-raw/ --recursive

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov
