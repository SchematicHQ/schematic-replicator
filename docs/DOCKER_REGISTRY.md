# Docker Image Repository Configuration

## Current Setup
The GitHub Actions workflow is currently configured to use GitHub Container Registry (GHCR) as the default image repository. This provides a good starting point with excellent integration to GitHub workflows.

## Repository Options for Production

### 1. GitHub Container Registry (GHCR) - Current Default
- **Pros**: Native GitHub integration, free for public repos, secure
- **Cons**: Tied to GitHub ecosystem
- **URL**: `ghcr.io/schematichq/schematic-datastream-replicator`

### 2. AWS Elastic Container Registry (ECR)
- **Pros**: Excellent AWS integration, enterprise-grade security, lifecycle policies
- **Cons**: AWS-specific, additional costs
- **Configuration**: Update `REGISTRY` env var to ECR URL in workflow

### 3. Docker Hub
- **Pros**: Most widely used, good caching, familiar to developers
- **Cons**: Rate limiting, costs for private repos
- **Configuration**: Update `REGISTRY` to `docker.io`

### 4. Google Container Registry (GCR) / Artifact Registry
- **Pros**: Good GCP integration, vulnerability scanning
- **Cons**: GCP-specific
- **Configuration**: Update `REGISTRY` to GCR/AR URL

## Third-Party Deployment Considerations

The current Docker image is designed to be production-ready for third-party deployment:

- **Multi-architecture support**: Built for linux/amd64 and linux/arm64
- **Security-focused**: Uses distroless base image, runs as non-root user
- **Health checks**: Exposes configurable health endpoints
- **OCI compliant**: Proper labels and metadata
- **Vulnerability scanning**: Trivy integration for security assessment
- **SBOM generation**: Software Bill of Materials for supply chain security

## Configuration Changes Needed

To switch registries, update these values in `.github/workflows/release-docker-image.yml`:

```yaml
env:
  REGISTRY: your-registry-url-here  # e.g., your-account.dkr.ecr.us-west-2.amazonaws.com
  IMAGE_NAME: schematichq/schematic-datastream-replicator
```

And ensure proper authentication is configured in the workflow.