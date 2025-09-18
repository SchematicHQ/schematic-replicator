# Makefile for schematic-datastream-replicator
# Provides convenient commands for development and deployment

.PHONY: help build build-docker run test clean docker-up docker-down docker-logs security-scan

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Build commands
build: ## Build the Go binary locally
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
		-ldflags='-w -s -extldflags "-static"' \
		-a -installsuffix cgo \
		-o schematic-datastream-replicator \
		.

build-docker: ## Build Docker image with security checks
	./scripts/build-docker.sh

build-docker-fast: ## Build Docker image without security checks
	./scripts/build-docker.sh --no-scan --no-test

# Development commands
run: ## Run the application locally
	go run .

test: ## Run Go tests
	go test -v ./...

clean: ## Clean build artifacts
	rm -f schematic-datastream-replicator
	go clean

# Docker Compose commands
docker-up: ## Start services with docker compose
	docker compose -f deployments/docker-compose.yml up -d

docker-down: ## Stop services with docker compose
	docker compose -f deployments/docker-compose.yml down

docker-logs: ## Show docker compose logs
	docker compose -f deployments/docker-compose.yml logs -f

docker-restart: ## Restart services
	docker compose -f deployments/docker-compose.yml restart

docker-clean: ## Clean up docker resources
	docker compose -f deployments/docker-compose.yml down -v --rmi all

# Static analysis and security
lint: ## Run linters
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, skipping..."; \
		go vet ./...; \
		go fmt ./...; \
	fi

security-scan: ## Run security analysis
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not installed, install with: go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest"; \
	fi

# Development workflow
dev-setup: ## Set up local development environment (one-time setup)
	./scripts/setup-local-dev.sh

dev-build: ## Build and restart development stack
	@echo "Building and restarting development stack..."
	./scripts/dev-build.sh --detached

dev-rebuild: ## Force rebuild and restart development stack
	@echo "Force rebuilding and restarting development stack..."
	./scripts/dev-build.sh --force-rebuild --detached

dev-logs: ## View development logs
	docker compose -f deployments/docker-compose.yml logs -f

dev-exec: ## Execute shell in development container
	@echo "Opening shell in development container..."
	docker compose -f deployments/docker-compose.yml exec schematic-replicator sh || echo "Container not running - start with 'make dev-build'"

dev-down: ## Stop development environment
	@echo "Stopping development environment..."
	docker compose -f deployments/docker-compose.yml down

dev-clean: ## Clean development environment (nuclear option)
	@echo "Cleaning development environment..."
	docker compose -f deployments/docker-compose.yml down --rmi all --volumes --remove-orphans || true
	docker image prune -f

# System checks
docker-check: ## Check Docker Compose compatibility
	@echo "Checking Docker Compose compatibility..."
	./scripts/check-docker-compose.sh

# Health checks
health-check: ## Check if the service is healthy
	@curl -f http://localhost:8090/health || echo "Health check failed"

ready-check: ## Check if the service is ready
	@curl -f http://localhost:8090/ready || echo "Ready check failed"

# Production deployment helpers
deploy-prod: build-docker ## Build and prepare for production deployment
	@echo "Production deployment preparation complete"
	@echo "Docker image: schematic-datastream-replicator:latest"

# Composite commands for common workflows
quick: build test dev-build ## Quick: build, test, and start development stack

fresh: dev-down dev-clean dev-build ## Fresh: clean everything and rebuild from scratch