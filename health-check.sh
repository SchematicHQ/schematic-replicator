#!/bin/bash

# health-check.sh - External health check script for schematic-datastream-replicator
# This script demonstrates proper external health checking

set -euo pipefail

# Configuration
HEALTH_PORT="${HEALTH_PORT:-8090}"
TIMEOUT="${TIMEOUT:-5}"
VERBOSE="${VERBOSE:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    if [ "$VERBOSE" = "true" ]; then
        echo -e "${GREEN}[HEALTH]${NC} $1"
    fi
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if curl is available
if ! command -v curl &> /dev/null; then
    error "curl is required but not installed"
    exit 1
fi

# Perform health check
check_endpoint() {
    local endpoint="$1"
    local description="$2"
    
    log "Checking $description..."
    
    if curl --fail --silent --max-time "$TIMEOUT" "http://localhost:$HEALTH_PORT$endpoint" > /dev/null; then
        if [ "$VERBOSE" = "true" ]; then
            echo -e "${GREEN}✓${NC} $description: OK"
        fi
        return 0
    else
        error "$description: FAILED"
        return 1
    fi
}

# Main health check
main() {
    log "Starting health check on port $HEALTH_PORT..."
    
    local health_ok=true
    local ready_ok=true
    
    # Check liveness
    if ! check_endpoint "/health" "Liveness probe"; then
        health_ok=false
    fi
    
    # Check readiness
    if ! check_endpoint "/ready" "Readiness probe"; then
        ready_ok=false
    fi
    
    # Summary
    if [ "$health_ok" = "true" ] && [ "$ready_ok" = "true" ]; then
        echo -e "${GREEN}✓ All health checks passed${NC}"
        exit 0
    elif [ "$health_ok" = "true" ]; then
        warn "Application is alive but not ready"
        exit 1
    else
        error "Application health check failed"
        exit 1
    fi
}

# Handle command line arguments
case "${1:-check}" in
    "check"|"")
        main
        ;;
    "liveness")
        check_endpoint "/health" "Liveness probe"
        ;;
    "readiness")
        check_endpoint "/ready" "Readiness probe"
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [check|liveness|readiness|help]"
        echo ""
        echo "External health check script for schematic-datastream-replicator"
        echo ""
        echo "Commands:"
        echo "  check      Check both liveness and readiness (default)"
        echo "  liveness   Check only liveness probe (/health)"
        echo "  readiness  Check only readiness probe (/ready)"
        echo "  help       Show this help message"
        echo ""
        echo "Environment variables:"
        echo "  HEALTH_PORT  Health check port (default: 8090)"
        echo "  TIMEOUT      Request timeout in seconds (default: 5)"
        echo "  VERBOSE      Enable verbose output (default: false)"
        echo ""
        echo "Examples:"
        echo "  $0                     # Check both endpoints"
        echo "  $0 liveness           # Check only liveness"
        echo "  HEALTH_PORT=9090 $0   # Use custom port"
        echo "  VERBOSE=true $0       # Enable verbose output"
        exit 0
        ;;
    *)
        error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac