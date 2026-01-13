# Schematic Datastream Replicator

A high-performance, production-ready service that replicates Schematic data to Redis cache for ultra-fast lookups, serving as a caching proxy between applications and the Schematic API. 

## 🚀 Quick Start

### Customer Installation (Docker)

Pull and run the latest version from Docker Hub:

```bash
# Pull the latest image
docker pull getschematic/schematic-replicator:latest

# Run with your Schematic API key and Redis connection
docker run -d \
  --name schematic-replicator \
  -p 8090:8090 \
  -e SCHEMATIC_API_KEY="your-api-key-here" \
  -e REDIS_URL="redis://your-redis-host:6379" \
  getschematic/schematic-replicator:latest

# Check health status
curl http://localhost:8090/health
```

### Development Setup

For detailed development setup, see [docs/DEV-README.md](docs/DEV-README.md).
For Docker-specific instructions, see [docs/DOCKER.md](docs/DOCKER.md).

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

### WebSocket Keepalive Configuration
Configure ping/pong intervals to handle load balancer timeouts:

- `WS_PING_INTERVAL`: How often to send WebSocket pings (default: `30s`, format: `20s`, `45s`, etc.)
- `WS_PONG_WAIT`: How long to wait for pong response before considering connection dead (default: `40s`)

**Load Balancer Timeout Guidelines**:
- Most cloud load balancers: Keep defaults (30s ping, 40s pong for 60s LB timeout)
- Aggressive LB (30s timeout): Use `WS_PING_INTERVAL=15s WS_PONG_WAIT=25s`
- Relaxed LB (120s+ timeout): Use `WS_PING_INTERVAL=50s WS_PONG_WAIT=60s`

Example for aggressive load balancer:
```bash
export WS_PING_INTERVAL="15s"
export WS_PONG_WAIT="25s"
```

### Async Processing Configuration (Performance Tuning)
These settings allow you to optimize performance for your specific infrastructure and workload:

- `NUM_WORKERS`: Number of worker goroutines per entity type (default: auto-detected = CPU cores, capped at 2-16 range)
- `BATCH_SIZE`: Messages processed per batch for Redis operations (default: `5` - optimized for low latency)
- `BATCH_TIMEOUT`: Maximum wait time before processing partial batches (default: `10ms` - prioritizes responsiveness)
- `COMPANY_CHANNEL_SIZE`: Buffer size for company message queue (default: `200`)
- `USER_CHANNEL_SIZE`: Buffer size for user message queue (default: `200`)
- `FLAGS_CHANNEL_SIZE`: Buffer size for flags message queue (default: `50`)
- `CIRCUIT_BREAKER_THRESHOLD`: Redis failures before circuit breaker opens (default: `3`)
- `CIRCUIT_BREAKER_TIMEOUT`: Circuit breaker recovery timeout (default: `15s`)

**Performance Guidelines**:
- **Low Latency**: Use smaller batch sizes (1-5) and shorter timeouts (5-15ms)
- **High Throughput**: Use larger batch sizes (10-50) and longer timeouts (25-100ms)
- **Memory Constrained**: Reduce channel sizes (50-100 each)
- **High CPU**: Increase worker count up to 2x CPU cores

### Async Initial Loading Configuration (New!)
For faster connection setup and improved resilience with large datasets (>1000 companies/users):

- `USE_ASYNC_LOADING`: Enable asynchronous initial data loading (`true`/`false`, default: `false`)
- `ASYNC_LOADER_PAGE_SIZE`: Page size for initial data loading (default: `100`)
- `ASYNC_LOADER_CIRCUIT_BREAKER_THRESHOLD`: API call failures before circuit breaker opens (default: `5`)
- `ASYNC_LOADER_CIRCUIT_BREAKER_TIMEOUT`: Circuit breaker recovery timeout (default: `30s`)

**When to use Async Loading**:
- ✅ **Large datasets** (2000+ companies/users): Reduces connection time from ~60s to ~2s
- ✅ **Production environments**: WebSocket connects quickly, data loads in background
- ✅ **High availability**: Circuit breaker protects against API overload
- ❌ **Small datasets** (<500 companies/users): Synchronous loading is sufficient
- ❌ **Development**: Synchronous loading provides simpler debugging

Example for production with large datasets:
```bash
export USE_ASYNC_LOADING="true"
export ASYNC_LOADER_PAGE_SIZE="100"
export ASYNC_LOADER_CIRCUIT_BREAKER_THRESHOLD="5"
export ASYNC_LOADER_CIRCUIT_BREAKER_TIMEOUT="30s"
```

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

## Usage

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

### Customer Deployment Examples

#### Small Scale / Low Resource Environment
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
export NUM_WORKERS="2"                    # Minimal workers for small systems
export BATCH_SIZE="3"                     # Small batches for low latency
export BATCH_TIMEOUT="5ms"                # Very responsive
export COMPANY_CHANNEL_SIZE="50"          # Small memory footprint
export USER_CHANNEL_SIZE="50"
export FLAGS_CHANNEL_SIZE="25"
./schematic-datastream-replicator
```

#### High Traffic / Low Latency Environment
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
export NUM_WORKERS="8"                    # More workers for high concurrency
export BATCH_SIZE="5"                     # Balanced for latency
export BATCH_TIMEOUT="10ms"               # Default responsive setting
export COMPANY_CHANNEL_SIZE="500"         # Larger buffers for traffic spikes
export USER_CHANNEL_SIZE="500"
export FLAGS_CHANNEL_SIZE="100"
export CIRCUIT_BREAKER_THRESHOLD="5"      # More tolerance for transient failures
./schematic-datastream-replicator
```

#### High Throughput / Resource Rich Environment
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export REDIS_ADDR="localhost:6379"
export NUM_WORKERS="12"                   # Maximum workers
export BATCH_SIZE="20"                    # Larger batches for throughput
export BATCH_TIMEOUT="50ms"               # Allow batching for efficiency
export COMPANY_CHANNEL_SIZE="1000"        # Large buffers
export USER_CHANNEL_SIZE="1000"
export FLAGS_CHANNEL_SIZE="200"
export CIRCUIT_BREAKER_TIMEOUT="30s"      # Longer recovery time
./schematic-datastream-replicator
```

### With Debug Logging
```bash
export SCHEMATIC_API_KEY="your-api-key-here"
export LOG_LEVEL="debug"
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

## Client Integration

### DataStream

The Schematic Go client can be configured to work with the replicator service for ultra-fast feature flag evaluations.

#### Replicator Mode

When running an external schematic-datastream-replicator service, you can configure your Schematic client to use replicator mode. In this mode, the client connects to the replicator service instead of directly to Schematic's WebSocket API, and the replicator handles all data streaming and caching automatically.

##### Example Usage

```go
package main

import (
    "context"
    "log"
    
    schematic "github.com/SchematicHQ/schematic-go"
    "github.com/SchematicHQ/schematic-go/core"
)

func main() {
    client := schematic.NewClient(
        core.WithAPIKey("your-api-key"),
        core.WithDatastream(
            core.WithReplicatorMode(),
        ),
    )
    
    // Client will now use replicator mode for all feature flag evaluations
    ctx := context.Background()
    flagValue, err := client.Features.CheckFlag(ctx, &core.CheckFlagRequestBody{
        Flag: "my-flag",
        Company: core.EntityInput{
            Keys: map[string]string{"id": "company-123"},
        },
    })
    
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Flag value: %v", flagValue.Flag)
}
```

##### Advanced Configuration (Optional)

The client automatically configures sensible defaults for replicator mode, but you can customize the configuration if needed:

```go
client := schematic.NewClient(
    core.WithAPIKey("your-api-key"),
    core.WithDatastream(
        core.WithReplicatorMode(),
        core.WithReplicatorHealthURL("http://my-replicator:8090/ready"),
        core.WithReplicatorHealthInterval(60*time.Second),
    ),
)
```

##### Default Configuration

- **Replicator Health URL**: `http://localhost:8090/ready`
- **Health Check Interval**: 30 seconds
- **Cache TTL**: 24 hours (handled automatically by the replicator)

## License

This project follows the same license as the parent Schematic repository.
