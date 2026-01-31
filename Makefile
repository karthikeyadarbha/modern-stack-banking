# Makefile for Modern Stack Banking (Argus-Lakehouse)

# Variables
PYTHON = python3
PIP = pip3
VENV = .venv

# Colors for pretty printing
GREEN = \033[0;32m
NC = \033[0m # No Color

.PHONY: help setup clean run-ingest check-silver

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  setup       Create virtual env and install dependencies"
	@echo "  clean       Remove all generated data (Bronze/Silver/Gold)"
	@echo "  run-ingest  Run PySpark ETL to ingest data"
	@echo "  check-data  Run DuckDB to query the Data Lake locally"

setup: ## Install dependencies from requirements.txt
	@echo "${GREEN}📦 Setting up environment...${NC}"
	$(PIP) install -r requirements.txt
	@echo "${GREEN}✅ Dependencies installed.${NC}"

clean: ## Wipes the local 'S3' buckets
	@echo "${GREEN}🧹 Cleaning up local data warehouse...${NC}"
	rm -rf data/warehouse/*
	rm -rf data/landing/*
	mkdir -p data/warehouse data/landing
	@echo "${GREEN}✨ Clean complete.${NC}"

run-ingest: ## Runs the Spark ETL job
	@echo "${GREEN}🚀 Launching Spark Ingestion Job...${NC}"
	PYTHONPATH=. $(PYTHON) src/jobs/etl_ingest.py

check-data: ## Runs DuckDB local inspection
	@echo "${GREEN}🦆 Querying Iceberg with DuckDB...${NC}"
	$(PYTHON) src/debug/check_data.py