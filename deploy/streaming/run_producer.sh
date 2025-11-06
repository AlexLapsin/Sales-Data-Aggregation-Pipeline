#!/bin/bash
# Run Kafka producer for streaming test events
# Usage: ./run_producer.sh [count] [interval]

set -euo pipefail

# Default values
COUNT="${1:-1000}"
INTERVAL="${2:-0.1}"
MODE="${3:-burst}"

CONTAINER="sales_data_aggregation_pipeline-etl-pipeline-1"
BROKER="kafka:29092"

echo "=========================================="
echo "Starting Kafka Producer"
echo "=========================================="
echo "Count: $COUNT events"
echo "Interval: ${INTERVAL}s"
echo "Mode: $MODE"
echo "Broker: $BROKER"
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "ERROR: Container $CONTAINER is not running"
    echo "Start it with: docker-compose up -d"
    exit 1
fi

# Run producer
if [ "$MODE" = "burst" ]; then
    docker exec "$CONTAINER" sh -c "cd /app && python src/streaming/sales_event_producer.py --brokers $BROKER --count $COUNT --burst"
else
    docker exec "$CONTAINER" sh -c "cd /app && python src/streaming/sales_event_producer.py --brokers $BROKER --count $COUNT --interval $INTERVAL"
fi

echo ""
echo "=========================================="
echo "Producer finished"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Verify Kafka Connect wrote to S3:"
echo "   aws s3 ls s3://raw-sales-pipeline-976404003846/sales_data/sales_events/ --recursive | tail -20"
echo ""
echo "2. Check connector status:"
echo "   curl -s http://localhost:8083/connectors/sales-events-s3-bronze-sink/status"
