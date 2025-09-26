#!/bin/bash
# Multi-architecture Docker build script for local development and manual builds
# This script builds the Docker image with proper cross-compilation to avoid QEMU issues
# Note: Production releases are handled automatically by GitHub Actions

set -euo pipefail

# Default values
IMAGE_NAME="${IMAGE_NAME:-getschematic/schematic-replicator}"
VERSION="${VERSION:-dev}"
REVISION="${REVISION:-$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')}"
BUILDTIME="${BUILDTIME:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
PUSH="${PUSH:-false}"
DOCKERFILE="${DOCKERFILE:-deployments/Dockerfile}"

# Build context is current directory (where go.mod and go.sum are located)
BUILD_CONTEXT="${BUILD_CONTEXT:-./}"

echo "Building multi-arch Docker image..."
echo "Image: $IMAGE_NAME"
echo "Version: $VERSION"
echo "Revision: $REVISION"
echo "Build time: $BUILDTIME"
echo "Platforms: $PLATFORMS"
echo "Dockerfile: $DOCKERFILE"
echo "Build context: $BUILD_CONTEXT"

# Create buildx builder if it doesn't exist
if ! docker buildx inspect multiarch >/dev/null 2>&1; then
    echo "Creating buildx builder..."
    docker buildx create --name multiarch --driver docker-container --use
    docker buildx inspect --bootstrap
else
    echo "Using existing buildx builder..."
    docker buildx use multiarch
fi

# Build command with proper build args
BUILD_CMD="docker buildx build"
BUILD_CMD="$BUILD_CMD --platform $PLATFORMS"
BUILD_CMD="$BUILD_CMD --file $DOCKERFILE"
BUILD_CMD="$BUILD_CMD --build-arg VERSION=$VERSION"
BUILD_CMD="$BUILD_CMD --build-arg REVISION=$REVISION"
BUILD_CMD="$BUILD_CMD --build-arg BUILDTIME=$BUILDTIME"

# Add tags
BUILD_CMD="$BUILD_CMD --tag $IMAGE_NAME:$VERSION"
BUILD_CMD="$BUILD_CMD --tag $IMAGE_NAME:latest"

# Add push flag if enabled
if [ "$PUSH" = "true" ]; then
    BUILD_CMD="$BUILD_CMD --push"
else
    BUILD_CMD="$BUILD_CMD --load"
fi

# Add build context
BUILD_CMD="$BUILD_CMD $BUILD_CONTEXT"

echo "Executing: $BUILD_CMD"
eval "$BUILD_CMD"

echo "Build completed successfully!"

# If not pushing, show the built images
if [ "$PUSH" = "false" ]; then
    echo "Built images:"
    docker images "$IMAGE_NAME"
fi