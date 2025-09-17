# Do## 🔗 URL CoYou can override this by setting `SCHEMATIC_DATASTREAM_URL` explicitly in your environment.

## 🏠 Local Development

When running the Docker container locally and connecting to a local API service running outside the container, you have several options:

### Option 1: Host Networking (Linux/macOS)
```bash
# Use host.docker.internal to access host services
SCHEMATIC_API_URL=http://host.docker.internal:8080 docker compose up
```

### Option 2: Docker Compose with Extra Hosts
Add to your `docker-compose.override.yml`:
```yaml
version: '3.8'
services:
  schematic-replicator:
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      - SCHEMATIC_API_URL=http://host.docker.internal:8080
```

### Option 3: Use Host IP Address
```bash
# Find your host IP and use it directly
SCHEMATIC_API_URL=http://192.168.1.100:8080 docker compose up
```

### Option 4: Network Mode Host (Linux only)
Add to your `docker-compose.override.yml`:
```yaml
version: '3.8'
services:
  schematic-replicator:
    network_mode: "host"
    environment:
      - SCHEMATIC_API_URL=http://localhost:8080
```

### Quick Setup for Local Development
```bash
# 1. Copy the example override file
cp docker-compose.override.yml.example docker-compose.override.yml

# 2. Edit the API URL to match your local service
# (default is http://host.docker.internal:8080)

# 3. Start the stack - it will automatically use the override
docker compose up
```

## 🔨 Development Build Scripts

### Quick Development Workflow
```bash
# Method 1: Using Make (recommended)
make dev-setup    # Set up local development (one-time)
make quick        # Build, test, and start stack

# Method 2: Using dev-build script directly
./dev-build.sh --detached

# Method 3: Using existing build-docker script
./build-docker.sh && docker compose up -d
```

### Available Scripts
- **`dev-build.sh`** - Complete development workflow (build → test → Docker → start)
- **`build-docker.sh`** - Production Docker build with security scanning
- **`setup-local-dev.sh`** - Interactive local development setup
- **`Makefile`** - Convenient make targets for all workflows

### Make Targets
```bash
make help          # Show all available commands
make dev-setup     # Set up local development
make quick         # Build, test, and start (most common)
make fresh         # Clean everything and rebuild
make dev-logs      # View container logs
make health-check  # Test health endpoints
```

**Note**: `host.docker.internal` works on Docker Desktop (macOS/Windows). On Linux, use `--add-host=host.docker.internal:host-gateway` or your actual host IP address.

### Testing Local Connection
```bash
# 1. Start your local API service (e.g., on port 8080)
# 2. Test connectivity from container
docker run --rm --add-host=host.docker.internal:host-gateway alpine/curl \
  curl -I http://host.docker.internal:8080/health

# 3. If successful, start the replicator stack
docker-compose up
```

## 🚀 Quick Startguration

The datastream WebSocket URL is automatically derived from the API URL by the Schematic Go client:
- `https://api.schematichq.com` → `wss://api.schematichq.com/datastream`
- `http://localhost:8080` → `ws://localhost:8080/datastream`

You can override this by setting `SCHEMATIC_DATASTREAM_URL` explicitly in your environment.ployment Guide

This directory contains Docker configuration for the Schematic Datastream Replicator with security best practices and production-ready setup.

## � URL Configuration

The datastream WebSocket URL is automatically derived from the API URL:
- `https://api.schematichq.com` → `wss://api.schematichq.com/datastream`
- `http://localhost:8080` → `ws://localhost:8080/datastream`

You can override this by setting `SCHEMATIC_DATASTREAM_URL` explicitly in your environment.

## �🚀 Quick Start

### Prerequisites
- Docker (20.10+)
- Docker Compose (2.0+)
- Go (1.21+) for building from source

### Build and Run

```bash
# Build with local dependencies (recommended)
./build-docker.sh

# Build standalone (published deps only)
./build-docker.sh standalone

# Run with Docker Compose (includes Redis)
```bash
SCHEMATIC_API_KEY=your-key docker compose up

# Or run standalone
docker run -p 8090:8090 -e SCHEMATIC_API_KEY=your-key schematic-datastream-replicator:latest
```

**Note:** Due to local Go module dependencies, use the parent directory as build context:
```bash
# Manual build with local dependencies
docker build -f schematic-datastream-replicator/Dockerfile -t schematic-datastream-replicator .
```

## 🏗️ Build Script

The `build-docker.sh` script implements security best practices and handles local dependencies:

```bash
# Basic build with local dependencies
./build-docker.sh

# Build standalone version (published deps only)
./build-docker.sh standalone

# Custom version and registry
VERSION=1.0.0 REGISTRY=myregistry.com ./build-docker.sh

# Skip tests and security scan
SKIP_TESTS=true SECURITY_SCAN=false ./build-docker.sh

# Test existing image
./build-docker.sh test

# Security scan only
./build-docker.sh scan

# Clean up intermediate images
./build-docker.sh clean
```

### Build Features
- ✅ **Security scanning** with Trivy and Docker Scout
- ✅ **Multi-stage build** for minimal image size
- ✅ **Vulnerability checks** before deployment
- ✅ **Automated testing** of built images
- ✅ **Go security analysis** with gosec

## 🔒 Security Features

### Dockerfile Security
- **Distroless base image** (`gcr.io/distroless/static-debian12:nonroot`)
- **Non-root user** (UID 65532)
- **Static binary** with security flags
- **Minimal attack surface** (no shell, package manager, etc.)
- **Security labels** for compliance
- **Health checks** for monitoring

### Runtime Security
- **Read-only filesystem** in docker-compose
- **No new privileges** security option
- **Temporary filesystem** for `/tmp`
- **Resource limits** on Redis
- **Network isolation** with custom bridge

## 📊 Image Details

| Aspect | Value |
|--------|-------|
| Base Image | `gcr.io/distroless/static-debian12:nonroot` |
| Image Size | ~15-20MB (minimal) |
| User | Non-root (UID 65532) |
| Exposed Ports | 8090 (health/API) |
| Health Check | Built-in HTTP endpoint |

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `SCHEMATIC_API_KEY` | Schematic API key | - | ✅ |
| `SCHEMATIC_API_URL` | Schematic API URL | `https://api.schematichq.com` | ❌ |
| | | *Local dev: `http://host.docker.internal:8080`* | |
| `SCHEMATIC_DATASTREAM_URL` | Datastream WebSocket URL | *Auto-derived from API URL* | ❌ |
| `CACHE_TTL` | Cache time-to-live | `5m` | ❌ |
| `LOG_LEVEL` | Log level (debug/info/warn/error) | `info` | ❌ |
| `HEALTH_PORT` | Health check port (configurable) | `8090` | ❌ |
| `REDIS_ADDR` | Redis address | `localhost:6380` | ❌ |
| `REDIS_PASSWORD` | Redis password | - | ❌ |
| `REDIS_DB` | Redis database number | `0` | ❌ |

### Docker Compose Configuration

```yaml
# .env file for docker-compose
SCHEMATIC_API_KEY=your-schematic-api-key
SCHEMATIC_API_URL=https://api.schematichq.com
CACHE_TTL=5m
LOG_LEVEL=info
REDIS_PASSWORD=your-secure-redis-password
```

**Notes**: 
- Redis is exposed on port **6380** (instead of the default 6379) to avoid conflicts with existing local Redis instances.
- Health check port is configurable via `HEALTH_PORT` environment variable (default: 8090)
- Health checks are handled externally via HTTP endpoints (no built-in Docker health checks)
- All services are grouped under the `schematic-datastream` stack with proper labeling for easy management.

## 🚀 Deployment Options

### 1. Docker Compose (Recommended)

```bash
# Create .env file
cat > .env << EOF
SCHEMATIC_API_KEY=your-key-here
CACHE_TTL=5m
LOG_LEVEL=info
EOF

# Start the schematic-datastream stack
docker compose up -d

# View logs for the entire stack
docker compose logs -f

# View logs for specific service
docker compose logs -f schematic-replicator

# Check stack status
docker compose ps

# Stop the entire stack
docker compose down

# Stop and remove volumes (careful - deletes Redis data)
docker compose down -v

# Use custom health port
HEALTH_PORT=9090 docker compose up -d

## 🏥 Health Checks

The application provides HTTP endpoints for health monitoring:

### Available Endpoints

- **`GET /health`** - Liveness probe (is the application running?)
- **`GET /ready`** - Readiness probe (is the application ready to serve traffic?)

### Manual Health Checks

#### Using curl directly
```bash
# Check if application is healthy
curl http://localhost:8090/health

# Check if application is ready
curl http://localhost:8090/ready

# Custom health port
curl http://localhost:9090/health
```

#### Using the provided health check script
```bash
# Check both liveness and readiness
./health-check.sh

# Check only liveness
./health-check.sh liveness

# Check only readiness  
./health-check.sh readiness

# Use custom port
HEALTH_PORT=9090 ./health-check.sh

# Verbose output
VERBOSE=true ./health-check.sh
```

### Orchestrator Integration

#### Docker Swarm
```yaml
healthcheck:
  test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8090/health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 10s
```

#### Kubernetes
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8090
  initialDelaySeconds: 10
  periodSeconds: 30
readinessProbe:
  httpGet:
    path: /ready
    port: 8090
  initialDelaySeconds: 5
  periodSeconds: 10
```

## 🚀 Deployment Options
docker-compose logs -f

# Stop services
docker-compose down
```

### 2. Standalone Docker

```bash
docker run -d \
  --name schematic-replicator \
  --restart unless-stopped \
  -p 8090:8090 \
  -e SCHEMATIC_API_KEY=your-key \
  -e CACHE_TTL=5m \
  --security-opt no-new-privileges:true \
  --read-only \
  --tmpfs /tmp:noexec,nosuid,size=10m \
  schematic-datastream-replicator:latest
```

### 3. Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schematic-replicator
spec:
  replicas: 2
  selector:
    matchLabels:
      app: schematic-replicator
  template:
    metadata:
      labels:
        app: schematic-replicator
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        readOnlyRootFilesystem: true
      containers:
      - name: replicator
        image: schematic-datastream-replicator:latest
        ports:
        - containerPort: 8090
        env:
        - name: SCHEMATIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: schematic-secret
              key: api-key
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
        resources:
          requests:
            memory: "64Mi"
            cpu: "50m"
          limits:
            memory: "128Mi"
            cpu: "200m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8090
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8090
          initialDelaySeconds: 5
          periodSeconds: 10
```

## 🔍 Health Checks

The application provides health endpoints:

```bash
# Check if service is healthy
curl http://localhost:8090/health

# Check if service is ready to receive traffic
curl http://localhost:8090/ready
```

Response format:
```json
{
  "status": "healthy",
  "ready": true,
  "connected": true,
  "components": {
    "datastream": "connected",
    "redis": "connected"
  },
  "timestamp": "2025-09-16T10:00:00Z"
}
```

## 🐛 Troubleshooting

### Common Issues

1. **Container fails to start**
   ```bash
   docker logs schematic-replicator
   ```

2. **Health check fails**
   ```bash
   # Check if port is accessible
   curl -v http://localhost:8090/health
   
   # Check container logs
   docker-compose logs schematic-replicator
   ```

3. **Redis connection issues**
   ```bash
   # Test Redis connectivity
   docker-compose exec redis redis-cli ping
   ```

4. **Memory issues**
   ```bash
   # Check resource usage
   docker stats
   ```

### Security Scanning

```bash
# Scan for vulnerabilities
trivy image schematic-datastream-replicator:latest

# Check for secrets in image
docker history schematic-datastream-replicator:latest --no-trunc
```

## 📈 Production Considerations

1. **Resource Limits**: Set appropriate CPU and memory limits
2. **Log Management**: Configure log rotation and centralized logging
3. **Monitoring**: Set up Prometheus metrics and alerts
4. **Backup**: Ensure Redis data is backed up if persistent
5. **Security**: Regular security scans and updates
6. **High Availability**: Run multiple replicas with load balancing

## 🛠️ Development

```bash
# Build development image
./build-docker.sh --tag dev

# Run development environment
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

## 📝 License

See the main project LICENSE file.