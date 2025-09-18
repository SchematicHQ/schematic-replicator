# Schematic Datastream Replicator

A high-performance, production-ready service that replicates Schematic data to Redis cache for ultra-fast lookups, serving as a caching proxy between applications and the Schematic API.

## 📁 Repository Structure

```
├── docs/                    # Documentation
│   ├── DEV-README.md       # Development guide
│   └── DOCKER.md           # Docker usage guide
├── scripts/                # Build and utility scripts
│   ├── build-docker.sh     # Docker image build script
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

For detailed development setup, see [docs/DEV-README.md](docs/DEV-README.md).
For Docker-specific instructions, see [docs/DOCKER.md](docs/DOCKER.md).

## ✨ Features

- **Real-time Data Sync**: Connects to Schematic's WebSocket datastream for live updates
- **Comprehensive Entity Support**: Handles companies, users, and feature flags
- **Redis Caching**: Configurable Redis support with automatic fallback to local cache
- **Intelligent Cache Management**: Implements TTL-based caching with stale data cleanup
- **Structured Logging**: Context-aware logging with configurable log levels
- **Graceful Shutdown**: Proper connection cleanup on termination signals
- **Environment Configuration**: Extensive configuration via environment variables

## Prerequisites

- Go 1.21 or later
- Docker and Docker Compose (for containerized deployment)
- Redis (optional, but recommended for production)
- Valid Schematic API key

## Environment Variables

### Required
- `SCHEMATIC_API_KEY`: Your Schematic API key

### Optional Configuration
- `SCHEMATIC_BASE_URL`: WebSocket endpoint (default: `ws://localhost:8080/datastream`)
- `CACHE_TTL`: Cache time-to-live (default: unlimited, format: `1h30m`, `45s`, `0s` for unlimited, etc.)
- `CACHE_CLEANUP_INTERVAL`: Cleanup stale cache entries interval (default: `1h`, format: `30m`, `2h`, `0s` to disable)
- `LOG_LEVEL`: Logging level - `debug`, `info`, `warn`, `error` (default: `info`)

### Redis Configuration

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

## Usage

### Basic Usage
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
./replicator
```

### With Redis Cache (unlimited cache)
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
# CACHE_TTL not set = unlimited cache (default)
./replicator
```

### With Redis Cache (custom TTL)
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
export CACHE_TTL="10m"
./replicator
```

### With Debug Logging
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export LOG_LEVEL="debug"
./replicator
```

### Production Configuration (with cache expiration)
```bash
export SCHEMATIC_API_KEY="your-production-api-key"
export SCHEMATIC_BASE_URL="wss://api.schematichq.com/datastream"
export REDIS_ADDR="your-redis-host:6379"
export REDIS_PASSWORD="your-redis-password"
export CACHE_TTL="1h"  # Set explicit TTL, or omit for unlimited cache
export CACHE_CLEANUP_INTERVAL="30m"  # Clean stale entries every 30 minutes
export LOG_LEVEL="info"
./replicator
```

### Unlimited Cache with Cleanup (Recommended)
```bash
export SCHEMATIC_API_KEY="your-api-key"
export REDIS_ADDR="localhost:6379"
# CACHE_TTL not set = unlimited cache (default)
export CACHE_CLEANUP_INTERVAL="1h"  # Clean up stale entries hourly (default)
./replicator
```

## Building

```bash
go mod tidy
go build -o replicator .
```

## Architecture

### Caching System
The application implements a sophisticated caching system inspired by schematic-go:

- **Cache Providers**: Generic interface supporting both Redis and local cache
- **Automatic Fallback**: Falls back to local cache if Redis is unavailable
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
- **WARN**: Non-fatal issues (Redis fallback, unknown entities)
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
- **Redis Failures**: Graceful fallback to local caching
- **Data Parsing Errors**: Logged without stopping the application
- **Cache Errors**: Logged with specific error details and cache keys

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
# Start local Redis (optional)
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
