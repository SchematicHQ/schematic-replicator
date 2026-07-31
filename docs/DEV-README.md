# Development Guide

Quick reference for local development with the Schematic Datastream Replicator.

Task automation lives in [`Taskfile.yml`](../Taskfile.yml) and runs via
[Task](https://taskfile.dev) (`brew install go-task`). Run `task` with no
arguments to list every available target.

## Quick Start

```bash
# 1. Set up local development (one-time)
task dev-setup

# 2. Build, test, and start the stack
task quick

# 3. View logs
task dev-logs

# 4. Check health
task health-check
```

## Prerequisites

- Docker & Docker Compose
- Go 1.26+ (see `go.mod`)
- [Task](https://taskfile.dev)
- `SCHEMATIC_API_KEY` environment variable

## Common Tasks

| Task | What it does |
|------|--------------|
| `task` | List all available tasks |
| `task build` | Build the Go binary locally |
| `task test` | Run Go tests |
| `task run` | Run the application directly (`go run .`) |
| `task lint` | Run golangci-lint, or fall back to `go vet` + `go fmt` |
| `task security-scan` | Run gosec, if installed |
| `task clean` | Remove build artifacts |

### Development stack

| Task | What it does |
|------|--------------|
| `task dev-setup` | One-time local setup (interactive) |
| `task dev-build` | Build and restart the stack, detached |
| `task dev-rebuild` | Force rebuild without cache, then restart |
| `task dev-logs` | Follow container logs |
| `task dev-exec` | Open a shell in the replicator container |
| `task dev-down` | Stop the stack |
| `task dev-clean` | Remove containers, images, and volumes |
| `task quick` | `build` + `test` + `dev-build` |
| `task fresh` | `dev-down` + `dev-clean` + `dev-build` |

### Docker Compose

All compose commands target `deployments/docker-compose.yml`.

| Task | What it does |
|------|--------------|
| `task docker-up` / `docker-down` | Start / stop services |
| `task docker-logs` | Follow logs |
| `task docker-restart` | Restart services |
| `task docker-clean` | Tear down including volumes and images |
| `task docker-check` | Verify Docker Compose version compatibility |
| `task build-docker` | Build the production image with security checks |

### Health

| Task | What it does |
|------|--------------|
| `task health-check` | `curl` the `/health` endpoint |
| `task ready-check` | `curl` the `/ready` endpoint |

## Scripts

Task targets wrap the scripts in [`scripts/`](../scripts). Call them directly
when you need options Task doesn't expose:

```bash
./scripts/dev-build.sh --help              # Show all options
./scripts/dev-build.sh --detached          # Run in background
./scripts/dev-build.sh --force-rebuild     # Force rebuild without cache
./scripts/dev-build.sh --skip-tests        # Skip Go tests
./scripts/dev-build.sh --skip-build        # Docker only, skip the Go build

./scripts/build-docker.sh                  # Build the image (default)
./scripts/build-docker.sh scan             # Security scan an existing image
./scripts/build-docker.sh test             # Test an existing image
./scripts/build-docker.sh clean            # Remove intermediate images
```

## Local API Connection

### Automatic setup

```bash
task dev-setup
# → Creates deployments/docker-compose.override.yml
# → Prompts for your local API URL
```

### Manual setup

```bash
cp deployments/docker-compose.override.yml.example \
   deployments/docker-compose.override.yml

# Edit the API URL to match your local service.
# Default: http://host.docker.internal:8080
```

The `task` targets and `scripts/dev-build.sh` add this file automatically when it
exists, so `task docker-up` and friends pick it up with no extra flags.

Running Compose by hand is the one case that needs care: Compose only auto-loads
`docker-compose.override.yml` during default file discovery, and passing `-f`
bypasses that. List it explicitly:

```bash
docker compose \
  -f deployments/docker-compose.yml \
  -f deployments/docker-compose.override.yml \
  up

# Or let Compose discover both files:
docker compose --project-directory deployments up
```

Common local URLs:

- `http://host.docker.internal:8080` (default)
- `http://host.docker.internal:3000` (Node.js)
- `http://192.168.1.100:8080` (specific IP)

## Monitoring

```bash
# Health endpoints
curl http://localhost:8090/health
curl http://localhost:8090/ready

# Container stats
docker compose -f deployments/docker-compose.yml ps
docker stats $(docker compose -f deployments/docker-compose.yml ps -q)

# Logs
docker compose -f deployments/docker-compose.yml logs -f schematic-replicator
docker compose -f deployments/docker-compose.yml logs -f redis
```

## Troubleshooting

1. **API connection fails** — check `SCHEMATIC_API_URL` and that your local service is up.
2. **Redis connection fails** — ensure the Redis container is healthy. Redis is
   mandatory; the app exits if it cannot connect. `REDIS_ADDR` takes bare
   `host:port`, not a `redis://` URL.
3. **"Could not acquire writer lock"** — another replicator instance is running
   against the same Redis. Only one writer is allowed; see the
   [single-writer constraint](../README.md#single-writer-constraint).
4. **Build fails** — run `task clean` and retry.
5. **Port conflicts** — check that 8090 (health) and 6380 (Redis) are free.

## Environment Variables

See the [README](../README.md#environment-variables) for the full set. The ones
that matter most in development:

- `SCHEMATIC_API_KEY` — required
- `SCHEMATIC_API_URL` — API URL (default: `https://api.schematichq.com`)
- `REDIS_ADDR` — Redis address as `host:port` (default: `localhost:6379`)
- `LOG_LEVEL` — `debug`, `info`, `warn`, `error` (default: `info`)
- `CACHE_TTL` — cache duration (default: unlimited)
- `HEALTH_PORT` — health server port (default: `8090`)
