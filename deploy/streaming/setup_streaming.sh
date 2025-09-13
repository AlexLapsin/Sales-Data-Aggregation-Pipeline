#!/bin/bash
# setup_streaming.sh
# Setup script for streaming components

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Setting up streaming components"
echo "==============================="

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo "Checking prerequisites..."

if ! command_exists python3; then
    echo "ERROR: Python 3 is required but not installed"
    exit 1
fi

if ! command_exists docker; then
    echo "ERROR: Docker is required but not installed"
    exit 1
fi

echo "Prerequisites check passed"

# Install Python dependencies
echo "Installing Python dependencies..."
cd "$SCRIPT_DIR"

if [[ -f "requirements.txt" ]]; then
    pip install -r requirements.txt
    echo "Dependencies installed"
else
    echo "WARNING: requirements.txt not found, skipping pip install"
fi

# Check Docker Compose
echo "Checking Docker Compose setup..."
cd "$PROJECT_ROOT"

if [[ -f "docker-compose.yml" ]]; then
    echo "docker-compose.yml found"

    # Start Kafka if requested
    if [[ "${1:-}" == "--start-kafka" ]]; then
        echo "Starting local Kafka cluster..."
        docker-compose --profile kafka-local up -d

        # Wait for Kafka to be ready
        echo "Waiting for Kafka to be ready..."
        sleep 15

        # Test Kafka connection
        if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo "Kafka cluster is ready"
        else
            echo "ERROR: Kafka cluster failed to start properly"
            docker-compose --profile kafka-local logs kafka
            exit 1
        fi
    fi
else
    echo "ERROR: docker-compose.yml not found in project root"
    exit 1
fi

# Run tests
echo "Running producer tests..."
cd "$SCRIPT_DIR"

if python test_producer.py; then
    echo "All tests passed"
else
    echo "Some tests failed"
fi

# Show usage examples
echo ""
echo "Setup complete!"
echo "=================="
echo ""
echo "Next steps:"
echo "1. Start Kafka (if not already running):"
echo "   docker-compose --profile kafka-local up -d"
echo ""
echo "2. Test the producer:"
echo "   cd streaming"
echo "   python kafka_producer.py --count 10 --verbose"
echo ""
echo "3. Monitor with Kafka UI:"
echo "   http://localhost:8090"
echo ""
echo "4. For AWS MSK deployment:"
echo "   cd ../infra"
echo "   export ENABLE_MSK=true"
echo "   terraform apply"
echo ""

# Optional: Show Kafka status if running
if docker ps --format "table {{.Names}}\t{{.Status}}" | grep -q kafka 2>/dev/null; then
    echo "Current Kafka services:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(kafka|zookeeper)" || echo "No Kafka containers running"
fi

echo "Ready to stream sales data!"
