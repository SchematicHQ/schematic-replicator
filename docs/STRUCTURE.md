# Repository Structure Guide

This document explains the organization of the Schematic Datastream Replicator repository.

## 📁 Directory Structure

### `/docs/` - Documentation
Contains all project documentation:
- `DEV-README.md` - Quick development setup guide
- `DOCKER.md` - Docker deployment and configuration guide  
- `STRUCTURE.md` - This file, explaining repository organization

### `/scripts/` - Build and Utility Scripts
All executable scripts for development and deployment:
- `build-docker.sh` - Main Docker image build script with security scanning
- `dev-build.sh` - Quick development build and restart script
- `setup-local-dev.sh` - One-time local development environment setup
- `check-docker-compose.sh` - Docker Compose version compatibility checker
- `health-check.sh` - External health check script for monitoring

### `/deployments/` - Deployment Configurations
Docker and deployment-related files:
- `Dockerfile` - Main production Docker image (uses local dependencies)
- `Dockerfile.standalone` - Alternative image for published dependencies only
- `docker-compose.yml` - Complete Docker Compose stack with Redis
- `docker-compose.override.yml.example` - Local development override template
- `.dockerignore` - Files excluded from Docker build context

### `/` (Root) - Source Code and Configuration
Core application files:
- `main.go` - Application entry point and HTTP server
- `handlers.go` - WebSocket message handlers and connection management
- `cache.go` - Cache abstraction layer and Redis implementation
- `logger.go` - Structured logging utilities
- `redis.go` - Redis client configuration and connection management
- `*_test.go` - Test files (cache_test.go, pagination_test.go, etc.)
- `go.mod` / `go.sum` - Go module definition and dependencies
- `Makefile` - Build automation and development tasks
- `.gitignore` - Version control ignore rules

## 🎯 Design Principles

### 1. **Separation of Concerns**
- **Source code** stays in root for Go tooling compatibility
- **Documentation** is centralized in `/docs/`
- **Scripts** are organized in `/scripts/` for easy discovery
- **Deployment configs** are isolated in `/deployments/`

### 2. **Development Workflow**
- Use `make help` to see available commands
- Scripts are referenced with `./scripts/` prefix in Makefile
- Docker files use relative paths from deployment directory
- All docker-compose commands specify the file path explicitly

### 3. **Production Readiness**
- Clean separation between development and production configs
- Binary builds are excluded from version control
- Security scanning integrated into Docker builds
- Health checks available for container orchestration

## 🔧 Common Tasks

### Development
```bash
make dev-setup    # One-time setup
make quick        # Build, test, and start
make dev-logs     # View logs
```

### Docker Operations
```bash
make build-docker    # Build production image
make docker-up       # Start with Docker Compose
make docker-logs     # View container logs
```

### Testing and Quality
```bash
make test           # Run tests
make security-scan  # Security analysis
make lint          # Code quality checks
```

## 📚 Further Reading

- [DEV-README.md](DEV-README.md) - Development setup and workflow
- [DOCKER.md](DOCKER.md) - Docker deployment guide
- [Main README.md](../README.md) - Project overview and features