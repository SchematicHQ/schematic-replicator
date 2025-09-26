#!/bin/bash
# Local development Docker build script
# This script builds the Docker image with local development defaults

set -e

# Get current git information for better local builds
VERSION=${1:-$(git describe --tags --always --dirty 2>/dev/null || echo "dev")}
REVISION=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILDTIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "Building Docker image for local development..."
echo "Version: $VERSION"
echo "Revision: $REVISION"
echo "Build time: $BUILDTIME"
echo

docker build \
    -f deployments/Dockerfile \
    --build-arg VERSION="$VERSION" \
    --build-arg REVISION="$REVISION" \
    --build-arg BUILDTIME="$BUILDTIME" \
    -t schematic-datastream-replicator:local \
    ../

echo
echo "✅ Docker image built successfully!"
echo "   Image: schematic-datastream-replicator:local"
echo
echo "To run the image:"
echo "   docker run --rm schematic-datastream-replicator:local --help"
echo
echo "For development with environment variables:"
echo "   docker run --rm -e SCHEMATIC_API_KEY=your-key schematic-datastream-replicator:local"