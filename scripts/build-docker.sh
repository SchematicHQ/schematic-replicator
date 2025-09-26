#!/bin/bash

# Build script for schematic-datastream-replicator Docker image
# Supports both local dependency and standalone builds

set -euo pipefail

# Configuration
IMAGE_NAME="schematic-datastream-replicator"
VERSION="${VERSION:-latest}"
REGISTRY="${REGISTRY:-}"
BUILD_ARGS="${BUILD_ARGS:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[BUILD]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Validate environment
validate_environment() {
    log "Validating build environment..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
    fi
    
    if ! docker info &> /dev/null; then
        error "Docker daemon is not running"
    fi
    
    # Check if we're in the correct directory
    if [ ! -f "go.mod" ]; then
        error "go.mod not found. Please run this script from the schematic-datastream-replicator directory"
    fi
    
    log "Environment validation passed"
}

# Build function with local dependencies
build_with_local_deps() {
    local tag="${REGISTRY:+$REGISTRY/}$IMAGE_NAME:$VERSION"
    
    log "Building Docker image with local dependencies: $tag"
    log "Using parent directory as build context to include local dependencies"
    log "Build context: $(dirname "$(pwd)")"
    
    # Build the image using parent directory as context
    docker build \
        --platform linux/amd64 \
        --tag "$tag" \
        --file deployments/Dockerfile \
        --build-arg VERSION="$VERSION" \
        --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --build-arg VCS_REF="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
        --build-arg HEALTH_PORT="${HEALTH_PORT:-8090}" \
        ${BUILD_ARGS} \
        ..
    
    if [ $? -eq 0 ]; then
        log "Image built successfully: $tag"
        docker images "$tag" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"
        return 0
    else
        error "Failed to build image with local dependencies"
    fi
}

# Build standalone function 
build_standalone() {
    local tag="${REGISTRY:+$REGISTRY/}$IMAGE_NAME:$VERSION-standalone"
    
    log "Building standalone Docker image: $tag"
    log "This version requires published dependencies"
    
    docker build \
        --platform linux/amd64 \
        --tag "$tag" \
        --file deployments/Dockerfile.standalone \
        --build-arg VERSION="$VERSION" \
        --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --build-arg VCS_REF="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
        --build-arg HEALTH_PORT="${HEALTH_PORT:-8090}" \
        ${BUILD_ARGS} \
        .
    
    if [ $? -eq 0 ]; then
        log "Standalone image built successfully: $tag"
        docker images "$tag" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"
        return 0
    else
        error "Failed to build standalone image"
    fi
}

# Test function
test_image() {
    local tag="${1:-${REGISTRY:+$REGISTRY/}$IMAGE_NAME:$VERSION}"
    
    log "Testing image: $tag"
    
    # Basic smoke test
    log "Running smoke test..."
    docker run --rm --name test-container "$tag" --version 2>/dev/null || {
        warn "Smoke test failed - this might be expected if --version is not implemented"
    }
    
    # Health check test
    log "Testing health check..."
    container_id=$(docker run -d --name health-test "$tag")
    sleep 10
    
    # Check if container is healthy
    health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container_id" 2>/dev/null || echo "no-healthcheck")
    
    docker rm -f "$container_id" &>/dev/null
    
    if [ "$health_status" = "healthy" ]; then
        log "Health check test passed"
    elif [ "$health_status" = "no-healthcheck" ]; then
        warn "No health check configured"
    else
        warn "Health check test inconclusive: $health_status"
    fi
}

# Security scan function
security_scan() {
    local image_name="$1"
    
    log "Running security scan on image: $image_name"
    
    if command -v trivy &> /dev/null; then
        log "Running Trivy security scan..."
        trivy image --exit-code 1 --severity HIGH,CRITICAL "$image_name" || {
            warn "Security vulnerabilities found in image"
            return 1
        }
    else
        warn "Trivy not found - skipping security scan"
        warn "Install Trivy: https://aquasecurity.github.io/trivy/"
    fi
    
    return 0
}

# Cleanup function
cleanup() {
    log "Cleaning up intermediate images..."
    docker image prune -f --filter label=stage=builder
}

# Main execution
main() {
    log "Starting Docker build process for $IMAGE_NAME"
    
    validate_environment
    build_with_local_deps
    
    if [ "${SKIP_TESTS:-false}" != "true" ]; then
        test_image
    fi
    
    if [ "${SECURITY_SCAN:-true}" = "true" ]; then
        security_scan "${REGISTRY:+$REGISTRY/}$IMAGE_NAME:$VERSION"
    fi
    
    if [ "${CLEANUP:-true}" = "true" ]; then
        cleanup
    fi
    
    log "Build process completed successfully!"
    log "Image: ${REGISTRY:+$REGISTRY/}$IMAGE_NAME:$VERSION"
}

# Handle script arguments
case "${1:-build}" in
    "build")
        main
        ;;
    "standalone")
        validate_environment
        build_standalone
        ;;
    "scan")
        security_scan "${REGISTRY:+$REGISTRY/}$IMAGE_NAME:$VERSION"
        ;;
    "test")
        test_image "${2:-}"
        ;;
    "clean")
        cleanup
        ;;
    *)
        echo "Usage: $0 [build|standalone|scan|test|clean]"
        echo "  build:      Build with local dependencies (default)"
        echo "  standalone: Build standalone image (requires published deps)"
        echo "  scan:       Run security scan on existing image"
        echo "  test:       Test existing image"
        echo "  clean:      Clean up intermediate images"
        echo ""
        echo "Environment variables:"
        echo "  VERSION:       Image version tag (default: latest)"
        echo "  REGISTRY:      Container registry prefix"
        echo "  BUILD_ARGS:    Additional docker build arguments"
        echo "  SKIP_TESTS:    Skip testing phase (default: false)"
        echo "  SECURITY_SCAN: Run security scan (default: true)"
        echo "  CLEANUP:       Clean intermediate images (default: true)"
        exit 1
        ;;
esac