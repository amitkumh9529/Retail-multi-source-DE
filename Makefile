.PHONY: help install test lint run-sources up down dbt-compile

help:
	@echo "RetailPulse ELT Pipeline Commands:"
	@echo "  make install      - Install Python dependencies"
	@echo "  make test         - Run unit and integration tests"
	@echo "  make lint         - Run ruff linter"
	@echo "  make run-sources  - Generate 3PL data, seed POS database, run mock API"
	@echo "  make up           - Start Airflow stack with docker-compose"
	@echo "  make down         - Stop Airflow stack"
	@echo "  make dbt-compile  - Compile dbt models"

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

lint:
	ruff check ingestion/ tests/

run-sources:
	python -m data_source.threepl_generator.generate
	python -m data_source.pos_supabase.02_seed

up:
	docker-compose -f docker/docker-compose.yml up -d

down:
	docker-compose -f docker/docker-compose.yml down

dbt-compile:
	cd dbt_retailpulse && dbt compile --profiles-dir .
