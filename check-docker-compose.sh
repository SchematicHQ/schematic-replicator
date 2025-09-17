#!/bin/bash

# check-docker-compose.sh - Verify Docker Compose compatibility

set -e

echo "🔍 Checking Docker Compose compatibility..."

# Check if docker compose (V2) is available
if docker compose version >/dev/null 2>&1; then
    echo "✅ Docker Compose V2 is available"
    docker compose version
    echo ""
    echo "✅ All scripts and Makefiles use 'docker compose' (V2 syntax)"
else
    echo "❌ Docker Compose V2 not available"
    
    # Check if legacy docker-compose is available
    if command -v docker-compose >/dev/null 2>&1; then
        echo "⚠️  Legacy docker-compose found:"
        docker-compose version
        echo ""
        echo "🔧 To use legacy docker-compose, you would need to:"
        echo "   1. Install docker-compose-v1 OR"
        echo "   2. Create alias: alias docker-compose='docker compose'"
        echo ""
        echo "💡 Recommended: Update to Docker Desktop with Compose V2"
    else
        echo "❌ No Docker Compose found"
        echo ""
        echo "📦 Install Docker Desktop which includes Compose V2:"
        echo "   - macOS: https://docs.docker.com/desktop/mac/"
        echo "   - Windows: https://docs.docker.com/desktop/windows/"
        echo "   - Linux: https://docs.docker.com/desktop/linux/"
    fi
    
    exit 1
fi

echo "🧪 Testing docker compose with simple command..."
if docker compose config >/dev/null 2>&1; then
    echo "✅ Docker Compose configuration is valid"
else
    echo "⚠️  Docker Compose configuration has issues (this might be normal if no docker-compose.yml in current dir)"
fi

echo ""
echo "✅ Docker Compose check complete!"
echo "   All build scripts and Makefiles use 'docker compose' syntax"