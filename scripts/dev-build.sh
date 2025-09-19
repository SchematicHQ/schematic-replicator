#!/bin/bash

# dev-build.sh - Quick development build and restart script
# Builds the application, rebuilds Docker image, and restarts the stack

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[DEV]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check if we're in the right directory
if [ ! -f "go.mod" ] || [ ! -f "deployments/Dockerfile" ]; then
    error "Please run this script from the schematic-datastream-replicator directory"
fi

# Parse arguments
SKIP_TESTS=false
SKIP_BUILD=false
DETACHED=false
FORCE_REBUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --detached|-d)
            DETACHED=true
            shift
            ;;
        --force-rebuild)
            FORCE_REBUILD=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --skip-tests      Skip Go tests before building"
            echo "  --skip-build      Skip Go build step (Docker only)"
            echo "  --detached, -d    Run Docker Compose in detached mode"
            echo "  --force-rebuild   Force rebuild without cache"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  SCHEMATIC_API_KEY     Required API key"
            echo "  SCHEMATIC_API_URL     API URL (default: https://api.schematichq.com)"
            echo "  LOG_LEVEL            Log level (default: info)"
            exit 0
            ;;
        *)
            warn "Unknown option: $1"
            shift
            ;;
    esac
done

# Check if docker-compose.override.yml exists (API key should be configured there)
if [ ! -f "deployments/docker-compose.override.yml" ]; then
    warn "No docker-compose.override.yml found. Run './scripts/setup-local-dev.sh' to configure API key and local settings"
    echo ""
    echo "Alternatively, you can set SCHEMATIC_API_KEY as an environment variable:"
    echo "  export SCHEMATIC_API_KEY=your_api_key_here"
    echo "  ./scripts/dev-build.sh"
    echo ""
fi

# Check if API key is configured (either in override file or environment)
if [ ! -f "deployments/docker-compose.override.yml" ] && [ -z "${SCHEMATIC_API_KEY:-}" ]; then
    error "SCHEMATIC_API_KEY must be configured either in docker-compose.override.yml or as an environment variable"
fi

log "Starting development build process..."

# Step 1: Run Go tests (unless skipped)
if [ "$SKIP_TESTS" = false ]; then
    log "Running Go tests..."
    if go test ./... -v; then
        info "✅ All tests passed"
    else
        error "❌ Tests failed - fix tests before building"
    fi
else
    warn "Skipping Go tests"
fi

# Step 2: Build Go application (unless skipped)
if [ "$SKIP_BUILD" = false ]; then
    log "Building Go application..."
    if go build -o schematic-datastream-replicator .; then
        info "✅ Go build successful"
    else
        error "❌ Go build failed"
    fi
else
    warn "Skipping Go build"
fi

# Step 3: Stop existing containers
log "Stopping existing containers..."
COMPOSE_FILES="-f deployments/docker-compose.yml"
if [ -f "deployments/docker-compose.override.yml" ]; then
    COMPOSE_FILES="$COMPOSE_FILES -f deployments/docker-compose.override.yml"
    info "Using docker-compose.override.yml for local configuration"
fi
docker compose $COMPOSE_FILES down --remove-orphans || warn "No existing containers to stop"

# Step 4: Build Docker image
log "Building Docker image..."
BUILD_ARGS=""
if [ "$FORCE_REBUILD" = true ]; then
    BUILD_ARGS="--no-cache"
    log "Force rebuild enabled - not using Docker cache"
fi

if docker compose $COMPOSE_FILES build $BUILD_ARGS; then
    info "✅ Docker image built successfully"
else
    error "❌ Docker build failed"
fi

# Step 5: Start the stack
log "Starting Docker Compose stack..."
COMPOSE_ARGS=""
if [ "$DETACHED" = true ]; then
    COMPOSE_ARGS="-d"
    log "Running in detached mode"
fi

if docker compose $COMPOSE_FILES up $COMPOSE_ARGS; then
    if [ "$DETACHED" = true ]; then
        info "✅ Stack started successfully in background"
        echo ""
        info "Useful commands:"
        info "  docker compose $COMPOSE_FILES logs -f                 # View all logs"
        info "  docker compose $COMPOSE_FILES logs -f schematic-replicator  # View app logs only"
        info "  curl http://localhost:8090/health      # Health check"
        info "  docker compose $COMPOSE_FILES down                    # Stop stack"
    else
        info "✅ Stack started successfully"
    fi
else
    error "❌ Failed to start Docker Compose stack"
fi

log "Development build complete!"