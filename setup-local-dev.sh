#!/bin/bash
# setup-local-dev.sh - Quick setup for local development

set -e

echo "🏠 Setting up Schematic Datastream Replicator for local development..."

# Check if docker-compose.override.yml already exists
if [ -f "docker-compose.override.yml" ]; then
    echo "⚠️  docker-compose.override.yml already exists. Backing up to docker-compose.override.yml.backup"
    cp docker-compose.override.yml docker-compose.override.yml.backup
fi

# Copy example override file
echo "📋 Creating docker-compose.override.yml from example..."
cp docker-compose.override.yml.example docker-compose.override.yml

# Prompt for local API URL
echo ""
echo "🔗 Configure your local API URL:"
echo "   Common options:"
echo "   - http://host.docker.internal:8080 (default)"
echo "   - http://host.docker.internal:3000 (Node.js/Express)"
echo "   - http://host.docker.internal:5000 (Python/Flask)"
echo "   - http://192.168.1.100:8080 (specific IP)"
echo ""
read -p "Enter your local API URL [http://host.docker.internal:8080]: " API_URL

# Use default if empty
if [ -z "$API_URL" ]; then
    API_URL="http://host.docker.internal:8080"
fi

# Update the override file with the provided URL
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s|http://host.docker.internal:8080|$API_URL|g" docker-compose.override.yml
else
    # Linux
    sed -i "s|http://host.docker.internal:8080|$API_URL|g" docker-compose.override.yml
fi

echo "✅ Configuration updated with API URL: $API_URL"
echo ""
echo "🚀 To start the development stack:"
echo "   docker-compose up"
echo ""
echo "🔍 To view logs:"
echo "   docker-compose logs -f schematic-replicator"
echo ""
echo "🏥 Health check:"
echo "   curl http://localhost:8090/health"
echo ""
echo "📝 Note: Make sure your local API service is running on $API_URL"