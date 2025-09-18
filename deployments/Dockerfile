# Multi-stage Dockerfile for schematic-datastream-replicator
# Build from parent directory with: docker build -f deployments/Dockerfile -t schematic-datastream-replicator .
# This approach includes all local dependencies in the build context

# Build args for version information (provided by CI/CD)
ARG BUILDTIME
ARG VERSION
ARG REVISION

# Build stage
FROM golang:1.25-alpine AS builder

# Install security updates and build dependencies
RUN apk update && apk add --no-cache \
    ca-certificates \
    git \
    && rm -rf /var/cache/apk/*

# Create non-root user for build
RUN adduser -D -g '' appuser

# Set working directory
WORKDIR /workspace

# Copy all source code (this assumes build context is the parent directory)
COPY . .

# Change to the replicator directory
WORKDIR /workspace/schematic-datastream-replicator

# Download dependencies (with local replace directives working)
RUN go mod download && go mod verify

# Build the application with security flags and version information
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s -extldflags '-static' -X main.version=${VERSION:-dev} -X main.commit=${REVISION:-unknown} -X main.buildTime=${BUILDTIME:-unknown}" \
    -a -installsuffix cgo \
    -o schematic-datastream-replicator \
    .

# Runtime stage using distroless for minimal attack surface
FROM gcr.io/distroless/static-debian12:nonroot

# Re-declare build args for runtime stage
ARG BUILDTIME
ARG VERSION
ARG REVISION

# Health port configuration (can be overridden at build time)
ARG HEALTH_PORT=8090
ENV HEALTH_PORT=${HEALTH_PORT}

# Copy CA certificates for HTTPS requests
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary from builder stage
COPY --from=builder /workspace/schematic-datastream-replicator/schematic-datastream-replicator /app/schematic-datastream-replicator

# Use non-root user (distroless nonroot user has UID 65532)
USER 65532:65532

# Set working directory
WORKDIR /app

# Expose health check port (configurable via HEALTH_PORT build arg)
EXPOSE ${HEALTH_PORT}

# No built-in health check - let orchestrators handle health checks via HTTP endpoints
# Health endpoints available at:
# - GET /health (liveness probe)
# - GET /ready (readiness probe)

# Set OCI labels for production readiness
LABEL \
    org.opencontainers.image.title="Schematic Datastream Replicator" \
    org.opencontainers.image.description="High-performance datastream replicator for Schematic" \
    org.opencontainers.image.vendor="Schematic" \
    org.opencontainers.image.licenses="MIT" \
    org.opencontainers.image.version="${VERSION:-dev}" \
    org.opencontainers.image.revision="${REVISION:-unknown}" \
    org.opencontainers.image.created="${BUILDTIME:-unknown}" \
    org.opencontainers.image.source="https://github.com/SchematicHQ/schematic-datastream-replicator" \
    security.scan.enabled="true"

# Run the application
ENTRYPOINT ["/app/schematic-datastream-replicator"]