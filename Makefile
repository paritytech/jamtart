.PHONY: help build run test clean start start-dev start-multi docker-build docker-run db-reset db-backup db-restore db-migrate db-migrate-revert db-shell db-status fmt clippy dev

# Default target
help:
	@echo "TART Telemetry System - Available Commands:"
	@echo ""
	@echo "Build & Run:"
	@echo "  make build          Build the TART backend in release mode"
	@echo "  make run            Run the TART backend"
	@echo "  make test           Run all tests"
	@echo "  make clean          Clean build artifacts and temporary files"
	@echo ""
	@echo "Start Scenarios:"
	@echo "  make start          Start TART with dashboard (no nodes)"
	@echo "  make start-dev      Start TART with 1 dev validator"
	@echo "  make start-multi    Start TART with 3 validators"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build   Build Docker image"
	@echo "  make docker-run     Run using Docker Compose"
	@echo ""
	@echo "Database (PostgreSQL):"
	@echo "  make db-status      Check PostgreSQL connection status"
	@echo "  make db-shell       Connect to PostgreSQL with psql"
	@echo "  make db-migrate     Run database migrations"
	@echo "  make db-migrate-revert  Revert last migration"
	@echo "  make db-reset       Drop and recreate database (WARNING: data loss)"
	@echo "  make db-backup      Create SQL backup in backups/ directory"
	@echo "  make db-restore     Restore from backup (Usage: make db-restore BACKUP=file.sql)"
	@echo ""
	@echo "Development:"
	@echo "  make fmt            Format code with cargo fmt"
	@echo "  make clippy         Run clippy linter"
	@echo "  make dev            Watch and auto-rebuild on changes"
	@echo ""

# Build the project
build:
	cargo build --release

# Run the backend
run: build
	./target/release/tart-backend

# Run tests
test:
	cargo test

# Clean build artifacts and temporary files
clean:
	cargo clean
	rm -rf /tmp/tart-telemetry.pids

# Start scenarios
start:
	./start-telemetry.sh

start-dev:
	./start-telemetry.sh -n 1

start-multi:
	./start-telemetry.sh -n 3

# Docker commands
docker-build:
	docker build -t tart-backend:latest .

docker-run:
	docker-compose up

# Development helpers
dev:
	cargo watch -x run

fmt:
	cargo fmt

clippy:
	cargo clippy -- -D warnings

# Database operations (PostgreSQL)
# These targets use environment variables or defaults from docker-compose.yml
POSTGRES_USER ?= tart
POSTGRES_PASSWORD ?= tart_password
POSTGRES_DB ?= tart_telemetry
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 5432
DATABASE_URL ?= postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)

db-reset:
	@echo "Resetting PostgreSQL database..."
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -U $(POSTGRES_USER) -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -d postgres -c "DROP DATABASE IF EXISTS $(POSTGRES_DB);" 2>/dev/null || true
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -U $(POSTGRES_USER) -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -d postgres -c "CREATE DATABASE $(POSTGRES_DB);"
	@echo "Database reset complete. Run migrations with: make db-migrate"

db-backup:
	@mkdir -p backups
	@echo "Backing up PostgreSQL database..."
	@PGPASSWORD=$(POSTGRES_PASSWORD) pg_dump -U $(POSTGRES_USER) -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) $(POSTGRES_DB) > backups/$(POSTGRES_DB)-$(shell date +%Y%m%d-%H%M%S).sql
	@echo "Database backed up to backups/"

db-restore:
	@if [ -z "$(BACKUP)" ]; then echo "Usage: make db-restore BACKUP=backups/filename.sql"; exit 1; fi
	@echo "Restoring database from $(BACKUP)..."
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -U $(POSTGRES_USER) -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) $(POSTGRES_DB) < $(BACKUP)
	@echo "Database restored from $(BACKUP)"

db-migrate:
	@echo "Running database migrations..."
	@cargo sqlx migrate run
	@echo "Migrations complete"

db-migrate-revert:
	@echo "Reverting last migration..."
	@cargo sqlx migrate revert
	@echo "Migration reverted"

db-shell:
	@echo "Connecting to PostgreSQL database..."
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -U $(POSTGRES_USER) -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) $(POSTGRES_DB)

db-status:
	@echo "Database connection status:"
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -U $(POSTGRES_USER) -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -d $(POSTGRES_DB) -c "SELECT version();" 2>/dev/null && echo "✓ Connected" || echo "✗ Failed to connect"