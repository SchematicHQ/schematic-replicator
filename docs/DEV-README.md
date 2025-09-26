# Development Guide

Quick reference for local development with the Schematic Datastream Replicator.

## 🚀 Quick Start

```bash
# 1. Set up local development (one-time)
make dev-setup

# 2. Quick build and start
make quick

# 3. View logs
make dev-logs

# 4. Check health
make health-check
```

## 📋 Prerequisites

- Docker & Docker Compose
- Go 1.21+
- `SCHEMATIC_API_KEY` environment variable

## 🔨 Build Scripts

### Primary Scripts
- **`make quick`** - Most common: build → test → Docker → start
- **`./dev-build.sh`** - Full development workflow with options
- **`./build-docker.sh`** - Production Docker build with security
- **`./setup-local-dev.sh`** - Interactive local API setup

### Script Options
```bash
# Development build with options
./dev-build.sh --help              # Show all options
./dev-build.sh --detached          # Run in background
./dev-build.sh --force-rebuild     # Force rebuild without cache
./dev-build.sh --skip-tests        # Skip Go tests

# Production build with options
./build-docker.sh                  # Default: build with local deps
./build-docker.sh standalone       # Build without local deps
./build-docker.sh scan             # Security scan only
```

## 🏠 Local API Connection

### Automatic Setup
```bash
./setup-local-dev.sh
# → Creates docker-compose.override.yml
# → Prompts for your local API URL
# → Ready to use with docker compose up
```

### Manual Setup
```bash
# Copy override template
cp docker-compose.override.yml.example docker-compose.override.yml

# Edit API URL to match your local service
# Default: http://host.docker.internal:8080
```

### Common Local URLs
- `http://host.docker.internal:8080` (default)
- `http://host.docker.internal:3000` (Node.js)
- `http://host.docker.internal:5000` (Python/Flask)
- `http://192.168.1.100:8080` (specific IP)

## 🔍 Development Workflow

### Standard Workflow
```bash
# Start development
make dev-setup     # One-time setup
make quick         # Build and start

# During development
make dev-logs      # View logs
make health-check  # Test endpoints
make dev-rebuild   # Rebuild after changes

# Clean up
make dev-down      # Stop containers
make dev-clean     # Full cleanup
```

### Testing Changes
```bash
# Quick iteration
./dev-build.sh --skip-tests --detached

# Full validation
make dev-rebuild

# Fresh start
make fresh
```

## 📊 Monitoring

```bash
# Health checks
curl http://localhost:8090/health
curl http://localhost:8090/ready

# Container stats
docker compose ps
docker stats $(docker compose ps -q)

# Logs
docker compose logs -f schematic-replicator
docker compose logs -f redis
```

## 🐛 Troubleshooting

### Common Issues
1. **API connection fails**: Check `SCHEMATIC_API_URL` and local service
2. **Redis connection fails**: Ensure Redis container is healthy
3. **Build fails**: Run `make clean` and try again
4. **Port conflicts**: Check if ports 8090/6380 are available

### Debug Commands
```bash
# Check environment
make env-check

# Container shell access
make dev-shell

# View all make targets
make help
```

## 📝 Environment Variables

Required:
- `SCHEMATIC_API_KEY` - Your API key

Optional:
- `SCHEMATIC_API_URL` - API URL (default: production)
- `LOG_LEVEL` - debug, info, warn, error (default: info)
- `CACHE_TTL` - Cache duration (default: 5m)
- `HEALTH_PORT` - Health check port (default: 8090)