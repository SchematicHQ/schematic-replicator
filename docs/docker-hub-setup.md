# Docker Hub Integration Setup

This document explains how to configure Docker Hub for automatic image publishing.

## Prerequisites

1. **Docker Hub Account**: Create an organization account at [hub.docker.com](https://hub.docker.com)
   - Recommended username: `schematichq`
   - This will allow images to be published as `schematichq/datastream-replicator`

2. **Docker Hub Repository**: Create a public repository named `datastream-replicator`
   - Navigate to: https://hub.docker.com/orgs/schematichq
   - Click "Create Repository"
   - Name: `datastream-replicator`
   - Visibility: Public (for customer distribution)

## GitHub Secrets Configuration

Add these secrets to your GitHub repository settings:

### Required Secrets

1. **DOCKERHUB_USERNAME**
   - Value: Your Docker Hub username (e.g., `schematichq`)
   - Path: Settings → Secrets and variables → Actions → New repository secret

2. **DOCKERHUB_TOKEN**
   - Create a Docker Hub Access Token:
     - Go to Docker Hub → Account Settings → Security → Access Tokens
     - Click "New Access Token"
     - Name: `GitHub Actions - datastream-replicator`
     - Permissions: `Read, Write, Delete`
     - Copy the generated token
   - Add as GitHub secret with the token value

## Image Publishing

Once configured, images will be automatically published to Docker Hub when you push tags:

```bash
# Create and push a release tag
git tag v1.0.0
git push origin v1.0.0
```

This will publish images with these tags:
- `schematichq/datastream-replicator:latest`
- `schematichq/datastream-replicator:v1.0.0`
- `schematichq/datastream-replicator:1.0.0`
- `schematichq/datastream-replicator:1`

## Customer Usage

Customers can pull and run your image with:

```bash
# Pull the latest version
docker pull schematichq/datastream-replicator:latest

# Pull a specific version
docker pull schematichq/datastream-replicator:v1.0.0

# Run the container
docker run -d \
  --name datastream-replicator \
  -p 8090:8090 \
  -e REDIS_URL=redis://localhost:6379 \
  schematichq/datastream-replicator:latest
```

## Multi-Platform Support

The workflow builds images for both:
- `linux/amd64` (Intel/AMD processors)
- `linux/arm64` (Apple Silicon, ARM servers)

## Security Features

- **Vulnerability Scanning**: Images are scanned with Trivy
- **SBOM Generation**: Software Bill of Materials for supply chain security
- **Minimal Image**: Uses distroless base for reduced attack surface
- **Non-root User**: Runs as user ID 65532 for security

## Monitoring

- Check build status in GitHub Actions
- View image details on Docker Hub
- Monitor pull statistics on Docker Hub dashboard

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify DOCKERHUB_USERNAME and DOCKERHUB_TOKEN secrets
   - Ensure Docker Hub token has write permissions

2. **Repository Not Found**
   - Create the repository on Docker Hub first
   - Ensure repository name matches workflow configuration

3. **Permission Denied**
   - Check Docker Hub token permissions
   - Verify organization membership if using organization account

### Logs

Check GitHub Actions logs for detailed error messages:
- Go to your repository → Actions tab
- Click on the failed workflow run
- Expand the failed step for details