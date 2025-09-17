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
	./build-docker.sh

build-docker-fast: ## Build Docker image without security checks
	./build-docker.sh --no-scan --no-test

# Development commands
run: ## Run the application locally
	go run .

test: ## Run Go tests
	go test -v ./...

test-race: ## Run Go tests with race detection
	go test -race -v ./...

bench: ## Run benchmarks
	go test -bench=. -benchmem ./...

# Docker commands
docker-up: ## Start services with docker compose
	docker compose up -d

docker-down: ## Stop services with docker compose
	docker compose down

docker-logs: ## Show docker compose logs
	docker compose logs -f

docker-restart: ## Restart services
	docker compose restart

docker-clean: ## Remove containers and images
	docker compose down -v --rmi all

# Security commands
security-scan: ## Run security scans on the codebase
	@echo "Running security scans..."
	@if command -v gosec >/dev/null 2>&1; then \
		echo "Running gosec..."; \
		gosec ./...; \
	else \
		echo "gosec not found, install with: go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest"; \
	fi
	@echo "Running go vet..."
	@go vet ./...
	@echo "Verifying modules..."
	@go mod verify

security-scan-image: ## Scan Docker image for vulnerabilities
	@if command -v trivy >/dev/null 2>&1; then \
		trivy image schematic-datastream-replicator:latest; \
	else \
		echo "trivy not found, install from: https://github.com/aquasecurity/trivy"; \
	fi

# Utility commands
clean: ## Clean build artifacts
	rm -f schematic-datastream-replicator
	go clean -cache
	docker system prune -f

fmt: ## Format Go code
	go fmt ./...

lint: ## Run golangci-lint
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found, install from: https://golangci-lint.run/usage/install/"; \
	fi

mod-tidy: ## Tidy Go modules
	go mod tidy
	go mod verify

# Development environment
dev-env-setup: ## Set up development tools (gosec, golangci-lint)
	@echo "Setting up development tools..."
	@if ! command -v gosec >/dev/null 2>&1; then \
		echo "Installing gosec..."; \
		go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest; \
	fi
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.54.2; \
	fi
	@echo "Development tools ready!"

# Local development commands
dev-setup: ## Set up local development environment
	@echo "Setting up local development..."
	./setup-local-dev.sh

dev-build: ## Build and restart development stack
	@echo "Building and restarting development stack..."
	./dev-build.sh --detached

dev-rebuild: ## Force rebuild and restart development stack
	@echo "Force rebuilding development stack..."
	./dev-build.sh --force-rebuild --detached

dev-logs: ## View development stack logs
	@echo "Viewing development logs (Ctrl+C to exit)..."
	docker compose logs -f

dev-shell: ## Open shell in running container
	@echo "Opening shell in development container..."
	docker compose exec schematic-replicator sh || echo "Container not running - start with 'make dev-build'"

dev-down: ## Stop development stack
	@echo "Stopping development stack..."
	docker compose down

dev-clean: ## Clean development environment
	@echo "Cleaning development environment..."
	docker compose down --rmi all --volumes --remove-orphans || true
	docker image prune -f

# System checks
docker-check: ## Check Docker Compose compatibility
	@echo "Checking Docker Compose compatibility..."
	./check-docker-compose.sh

# Health checks
health-check: ## Check if the service is healthy
	@curl -f http://localhost:8090/health || echo "Health check failed"

ready-check: ## Check if the service is ready
	@curl -f http://localhost:8090/ready || echo "Ready check failed"

# Production deployment helpers
deploy-prod: build-docker ## Build and prepare for production deployment
	@echo "Production deployment checklist:"
	@echo "✓ Docker image built with security checks"
	@echo "○ Update environment variables in .env"
	@echo "○ Configure resource limits"
	@echo "○ Set up monitoring and logging"
	@echo "○ Configure backup for Redis"
	@echo "○ Test health endpoints"

# All-in-one commands
all: clean security-scan build build-docker ## Run all quality checks and build everything

dev: mod-tidy fmt lint test build ## Complete development workflow

# Quick development workflows
quick: build test dev-build ## Quick: build, test, and start development stack

fresh: dev-down dev-clean dev-build ## Fresh: clean everything and rebuild from scratch