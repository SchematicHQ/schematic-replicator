# Schematic Datastream Replicator

A high-pe- Reliable Redis-based caching system Mandatory Redis caching for high-performance data storageformance, production-ready service that replicates Schematic data to Redis cache for ultra-fast lookups, serving as a caching proxy between applications and the Schematic API.

## 📁 Repository Structure

```
├── docs/                    # Documentation
│   ├── DEV-README.md       # Development guide
│   └── DOCKER.md           # Docker usage guide
├── scripts/                # Build and utility scripts
│   ├── build-docker.sh     # Production Docker image build script
│   ├── build-docker-local.sh # Local development Docker build script
│   ├── dev-build.sh        # Development build script
│   ├── setup-local-dev.sh  # Local development setup
│   ├── check-docker-compose.sh # Docker Compose compatibility check
│   └── health-check.sh     # Health check script
├── deployments/            # Deployment configurations
│   ├── Dockerfile          # Main Docker image
│   ├── Dockerfile.standalone # Standalone Docker image
│   ├── docker-compose.yml  # Docker Compose configuration
│   ├── docker-compose.override.yml.example # Local override example
│   └── .dockerignore       # Docker build ignore rules
├── main.go                 # Application entry point
├── handlers.go             # Message and connection handlers
├── cache.go               # Cache implementation
├── logger.go              # Logging utilities
├── redis.go               # Redis client configuration
├── *_test.go              # Test files
├── go.mod                 # Go module definition
├── go.sum                 # Go module checksums
├── Makefile               # Build and development tasks
└── README.md              # This file
```

## 🚀 Quick Start

### Customer Installation (Docker)

Pull and run the latest version from Docker Hub:

```bash
# Pull the latest image
docker pull schematichq/datastream-replicator:latest

# Run with your Schematic API key and Redis connection
docker run -d \
  --name schematic-replicator \
  -p 8090:8090 \
  -e SCHEMATIC_API_KEY="your-api-key-here" \
  -e REDIS_URL="redis://your-redis-host:6379" \
  schematichq/datastream-replicator:latest

# Check health status
curl http://localhost:8090/health
```

### Development Setup

For detailed development setup, see [docs/DEV-README.md](docs/DEV-README.md).
For Docker-specific instructions, see [docs/DOCKER.md](docs/DOCKER.md).

## ✨ Features

- **Real-time Data Sync**: Connects to Schematic's WebSocket datastream for live updates
- **Comprehensive Entity Support**: Handles companies, users, and feature flags
- **Redis Caching**: For all deployments, The application requires Redis and will not start without a successful Redis connection.
- **Intelligent Cache Management**: Implements TTL-based caching with stale data cleanup
- **Structured Logging**: Context-aware logging with configurable log levels
- **Graceful Shutdown**: Proper connection cleanup on termination signals
- **Environment Configuration**: Extensive configuration via environment variables

## Prerequisites

- Go 1.25.1 or later
- **Redis server** (required - application will not start without Redis connection)
- Docker and Docker Compose (for containerized deployment)
- Valid Schematic API key

## Environment Variables

### Required
- `SCHEMATIC_API_KEY`: Your Schematic API key

### Additional Configuration
- `SCHEMATIC_API_URL`: Schematic API base URL (default: `https://api.schematichq.com`)
- `SCHEMATIC_DATASTREAM_URL`: WebSocket datastream endpoint (default: auto-derived from API URL)
- `CACHE_TTL`: Cache time-to-live (default: unlimited, format: `1h30m`, `45s`, `0s` for unlimited, etc.)
- `CACHE_CLEANUP_INTERVAL`: Cleanup stale cache entries interval (default: `1h`, format: `30m`, `2h`, `0s` to disable)
- `LOG_LEVEL`: Logging level - `debug`, `info`, `warn`, `error` (default: `info`)
- `HEALTH_PORT`: Health server port (default: `8090`)

### Redis Configuration (Required)

**Note**: Redis is mandatory for the datastream replicator. The application will exit if it cannot connect to Redis.

#### Single Redis Instance
```bash
export REDIS_ADDR="localhost:6379"           # Redis server address
export REDIS_PASSWORD=""                     # Redis password (if required)
export REDIS_DB="0"                          # Redis database number
export REDIS_MAX_RETRIES="3"                # Maximum retry attempts
export REDIS_DIAL_TIMEOUT="5s"              # Connection timeout
export REDIS_READ_TIMEOUT="3s"              # Read timeout
export REDIS_WRITE_TIMEOUT="3s"             # Write timeout
```

#### Redis Cluster
```bash
export REDIS_CLUSTER_ADDRS="localhost:7000,localhost:7001,localhost:7002"
export REDIS_PASSWORD=""                     # Cluster password (if required)
export REDIS_MAX_REDIRECTS="8"              # Maximum cluster redirects
export REDIS_ROUTE_BY_LATENCY="true"        # Route by lowest latency
```

#### Quick Redis Setup
If you don't have Redis running locally:
```bash
# Using Docker
docker run -d -p 6379:6379 --name redis redis:alpine

# Using Homebrew (macOS)
brew install redis
brew services start redis

# Using apt (Ubuntu/Debian)
sudo apt install redis-server
sudo systemctl start redis-server
```

## Usage

### Basic Usage
```bash
# Ensure Redis is running (required)
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"  # Default Redis address
./schematic-datastream-replicator
```

### Docker Development
For local development with Docker:

```bash
# Build Docker image for local development (includes git info)
./scripts/build-docker-local.sh

# Run with basic configuration
docker run --rm \
  -e SCHEMATIC_API_KEY="your-api-key-here" \
  schematic-datastream-replicator:local

# Run with Redis (assumes Redis running on host)
docker run --rm \
  -e SCHEMATIC_API_KEY="your-api-key-here" \
  -e REDIS_ADDR="host.docker.internal:6379" \
  schematic-datastream-replicator:local
```

You can also build manually without git info:
```bash
# Simple Docker build (uses default version labels)
docker build -f deployments/Dockerfile -t schematic-datastream-replicator:local ../
```

### With Redis Cache (unlimited cache)
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
# CACHE_TTL not set = unlimited cache (default)
./schematic-datastream-replicator
```

### With Redis Cache (custom TTL)
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
export CACHE_TTL="10m"
./schematic-datastream-replicator
```

### With Debug Logging
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export LOG_LEVEL="debug"
./schematic-datastream-replicator
```

### Local Development (against localhost API)
```bash
export SCHEMATIC_API_KEY="your-dev-api-key"
export SCHEMATIC_API_URL="http://localhost:8080"  # Local API server
# WebSocket URL will be auto-derived as ws://localhost:8080/datastream
export LOG_LEVEL="debug"
./schematic-datastream-replicator
```

### Production Configuration (with cache expiration)
```bash
export SCHEMATIC_API_KEY="your-production-api-key"
export SCHEMATIC_API_URL="https://api.schematichq.com"  # Optional, this is the default
export REDIS_ADDR="your-redis-host:6379"
export REDIS_PASSWORD="your-redis-password"
export CACHE_TTL="1h"  # Set explicit TTL, or omit for unlimited cache
export CACHE_CLEANUP_INTERVAL="30m"  # Clean stale entries every 30 minutes
export LOG_LEVEL="info"
./schematic-datastream-replicator
```

### Unlimited Cache with Cleanup (Recommended)
```bash
export SCHEMATIC_API_KEY="your-api-key"
export REDIS_ADDR="localhost:6379"
# CACHE_TTL not set = unlimited cache (default)
export CACHE_CLEANUP_INTERVAL="1h"  # Clean up stale entries hourly (default)
./schematic-datastream-replicator
```

## Building

```bash
go mod tidy
go build -o schematic-datastream-replicator .
```

Or simply:
```bash
go build .  # Creates schematic-datastream-replicator binary
```

## URL Configuration

The application supports flexible URL configuration:

- **`SCHEMATIC_API_URL`**: Base API URL (default: `https://api.schematichq.com`)
- **`SCHEMATIC_DATASTREAM_URL`**: WebSocket endpoint (optional, auto-derived if not set)

### URL Auto-Derivation
If `SCHEMATIC_DATASTREAM_URL` is not explicitly set, the application automatically converts the API URL:
- `https://api.schematichq.com` → `wss://api.schematichq.com/datastream`
- `http://localhost:8080` → `ws://localhost:8080/datastream`

This means you typically only need to set `SCHEMATIC_API_URL` for both REST API and WebSocket connections.

## Architecture

### Caching System
The application implements a Redis-based caching system for high-performance data replication:

- **Redis-Only**: Uses Redis as the exclusive cache provider (no local cache fallback)
- **TTL Management**: Configurable time-to-live for cached entries
- **Paginated Data Loading**: Efficiently loads all companies and users through paginated API requests (100 items per page)
- **Stale Data Cleanup**: Removes outdated cache entries during bulk updates and periodic version cleanup
- **Version-Based Cleanup**: Automatically removes cache entries with old version keys to prevent memory leaks with unlimited cache
- **Key Management**: Uses hierarchical cache keys for efficient data organization

### Data Flow
1. **Connection Establishment**: WebSocket connects to Schematic datastream
2. **Initial Data Load**: Requests all flags, companies, and users on connection ready using paginated API calls (100 items per page)
3. **Real-time Updates**: Receives and processes individual entity updates
4. **Cache Synchronization**: Updates cache with new data and removes stale entries
5. **Error Handling**: Logs errors and maintains connection resilience

### Cache Key Structure
```
schematic:{entity-type}:{version}:{key}:{value}
```

Examples:
- `schematic:company:v1:id:company-123`
- `schematic:user:v1:email:user@example.com`
- `schematic:flags:v1:feature-flag-key`

### Cache Cleanup System

When using unlimited cache (default), the application includes an automatic cleanup mechanism to prevent memory leaks from stale cache entries:

**Version-Based Cleanup**: 
- Cache keys include a version component (e.g., `v1`, `v2`) based on the rules engine model structure
- When the rules engine version changes, old cache entries with outdated version keys become stale
- The cleanup manager periodically scans for and removes entries with old version keys
- Only cache entries matching the current version are retained

**Configuration**:
- `CACHE_CLEANUP_INTERVAL`: How often to run cleanup (default: `1h`)
- Set to `0s` to disable cleanup (not recommended with unlimited cache)
- Cleanup runs in the background without affecting performance

**Benefits**:
- Prevents memory bloat in Redis when using unlimited cache
- Automatically handles version transitions without manual intervention
- Maintains optimal cache performance by removing obsolete entries
- Safe operation that only removes confirmed stale data

## Message Types

### Supported Entity Types
- **flags**: Feature flag configurations
- **rulesengine.Company**: Individual company updates
- **rulesengine.Companies**: Bulk company data
- **rulesengine.User**: Individual user updates  
- **rulesengine.Users**: Bulk user data

### Datastream Actions
- **start**: Begin listening for entity type updates
- **stop**: Stop listening for entity type updates

## Logging

The application uses structured logging with the following levels:

- **DEBUG**: Detailed WebSocket and cache operations
- **INFO**: General application flow and statistics
- **WARN**: Non-fatal issues (unknown entities, configuration warnings)
- **ERROR**: Connection failures and data processing errors

Log format:
```
[LEVEL] message
```

Example output:
```
[INFO] Starting Schematic Datastream Replicator...
[INFO] Connecting to: ws://localhost:8080/datastream
[INFO] Using Redis cache
[INFO] Datastream connection ready, requesting all flags, companies and users...
[INFO] Received 15 flags
[INFO] Loaded 15 flags into cache
[INFO] Loading companies from Schematic API
[DEBUG] Fetching companies page: offset=0, limit=100
[DEBUG] Retrieved 100 companies from page (offset=0)
[DEBUG] Fetching companies page: offset=100, limit=100
[DEBUG] Retrieved 100 companies from page (offset=100)
[DEBUG] Fetching companies page: offset=200, limit=100
[DEBUG] Retrieved 34 companies from page (offset=200)
[DEBUG] Reached end of companies list (got 34 < 100)
[INFO] Successfully cached 234 companies across all pages
[INFO] Loading users from Schematic API  
[DEBUG] Fetching users page: offset=0, limit=100
[DEBUG] Retrieved 100 users from page (offset=0)
[...additional pages...]
[INFO] Successfully cached 1456 users across all pages
```

## Error Handling

- **Connection Failures**: Automatic reconnection with exponential backoff
- **Redis Failures**: Application will exit if Redis connection fails
- **Data Parsing Errors**: Logged without stopping the application
- **Cache Errors**: Logged with specific error details and cache keys

## Docker Image Versions

The application is distributed via Docker Hub with automatic semantic versioning:

### Available Tags
- `schematichq/datastream-replicator:latest` - Latest stable release
- `schematichq/datastream-replicator:v1.0.0` - Specific version (example)
- `schematichq/datastream-replicator:1.0.0` - Version without 'v' prefix
- `schematichq/datastream-replicator:1` - Major version

### Platform Support
Images are built for multiple architectures:
- `linux/amd64` - Intel/AMD processors
- `linux/arm64` - ARM processors (Apple Silicon, ARM servers)

### Image Features
- **Minimal Size**: Based on distroless images for security
- **Non-root User**: Runs as UID 65532 for enhanced security
- **Health Endpoints**: Built-in `/health` and `/ready` endpoints
- **Vulnerability Scanning**: All images are scanned for security issues

## Performance Considerations

- **Memory Usage**: Local cache is limited to 1000 entries by default
- **Network Efficiency**: Only requests data once per connection
- **Cache Efficiency**: Uses TTL expiration to balance freshness and performance
- **Batch Operations**: Processes bulk updates efficiently with minimal Redis calls

## Dependencies

- `github.com/redis/go-redis/v9`: Redis client library
- `github.com/schematichq/schematic-datastream-ws`: WebSocket client for Schematic
- `github.com/schematichq/rulesengine`: Schematic rules engine types

## Development

### Local Development
```bash
# Start local Redis (required)
docker run -d -p 6379:6379 redis:alpine

# Run with debug logging
export SCHEMATIC_API_KEY="your-dev-api-key"
export LOG_LEVEL="debug"
go run .
```

### Testing Redis Configuration
```bash
# Test Redis connection
redis-cli ping

# Monitor Redis operations
redis-cli monitor
```

## License

This project follows the same license as the parent Schematic repository.
