#!/bin/bash
# Deploy Kafka S3 Bronze Connector for Medallion Architecture
# This replaces the direct Snowflake connector with Bronze layer ingestion

set -euo pipefail

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTOR_CONFIG="$SCRIPT_DIR/s3_connector_config.json"

echo "==========================================="
echo "Deploying Kafka S3 Bronze Connector"
echo "==========================================="
echo "Kafka Connect URL: $KAFKA_CONNECT_URL"
echo "Connector Config: $CONNECTOR_CONFIG"
echo ""

# Function to check if Kafka Connect is available
check_kafka_connect() {
    echo "Checking Kafka Connect availability..."
    local retries=10
    local wait_time=5

    for ((i=1; i<=retries; i++)); do
        if curl -s -f "$KAFKA_CONNECT_URL" > /dev/null; then
            echo "SUCCESS: Kafka Connect is available"
            return 0
        else
            echo "WAITING: Attempt $i/$retries: Kafka Connect not ready, waiting ${wait_time}s..."
            sleep $wait_time
        fi
    done

    echo "ERROR: Kafka Connect is not available after $retries attempts"
    return 1
}

# Function to delete existing connector if it exists
delete_existing_connector() {
    local connector_name="sales-events-s3-bronze-sink"

    echo "Checking for existing connector: $connector_name"

    if curl -s -f "$KAFKA_CONNECT_URL/connectors/$connector_name" > /dev/null; then
        echo "Found existing connector, deleting..."
        curl -X DELETE "$KAFKA_CONNECT_URL/connectors/$connector_name"
        echo "SUCCESS: Existing connector deleted"
        sleep 2
    else
        echo "No existing connector found"
    fi
}

# Function to create Bronze connector
create_bronze_connector() {
    echo "Creating S3 Bronze connector..."

    local response
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d @"$CONNECTOR_CONFIG" \
        "$KAFKA_CONNECT_URL/connectors")

    if [[ $? -eq 0 ]]; then
        echo "SUCCESS: Bronze connector created successfully"
        echo "Response: $response"
    else
        echo "ERROR: Failed to create Bronze connector"
        echo "Response: $response"
        return 1
    fi
}

# Function to verify connector status
verify_connector() {
    local connector_name="sales-events-s3-bronze-sink"

    echo "Verifying connector status..."
    sleep 3

    local status
    status=$(curl -s "$KAFKA_CONNECT_URL/connectors/$connector_name/status")

    echo "Connector Status:"
    echo "$status" | jq '.' 2>/dev/null || echo "$status"

    # Check if connector is running
    local state
    state=$(echo "$status" | jq -r '.connector.state' 2>/dev/null || echo "unknown")

    if [[ "$state" == "RUNNING" ]]; then
        echo "SUCCESS: Connector is running successfully"
        return 0
    else
        echo "WARNING: Connector state: $state"
        return 1
    fi
}

# Function to show connector info
show_connector_info() {
    local connector_name="sales-events-s3-bronze-sink"

    echo ""
    echo "==========================================="
    echo "Bronze Connector Information"
    echo "==========================================="

    echo "Connector Config:"
    curl -s "$KAFKA_CONNECT_URL/connectors/$connector_name/config" | jq '.' 2>/dev/null || echo "Failed to get config"

    echo ""
    echo "Target S3 Structure:"
    echo "s3://raw-sales-pipeline-976404003846/streaming_events/YYYY/MM/dd/HH/"
    echo ""
    echo "Error Handling:"
    echo "- Dead Letter Queue: sales_events_dlq"
    echo "- Error tolerance: all"
    echo "- Error logging: enabled"
    echo ""
    echo "Data Format:"
    echo "- Format: JSON"
    echo "- Partitioning: Time-based (hourly)"
    echo "- Schema compatibility: None (schemaless)"
}

# Main execution
main() {
    echo "Starting Bronze connector deployment..."
    echo ""

    # Validate config file exists
    if [[ ! -f "$CONNECTOR_CONFIG" ]]; then
        echo "ERROR: Connector config file not found: $CONNECTOR_CONFIG"
        exit 1
    fi

    # Validate JSON (skip in Windows/Git Bash if jq has issues)
    if command -v jq >/dev/null 2>&1; then
        if ! jq empty "$CONNECTOR_CONFIG" 2>/dev/null; then
            echo "WARNING: JSON validation with jq failed, attempting deployment anyway..."
        fi
    else
        echo "WARNING: jq not found, skipping JSON validation"
    fi

    # Execute deployment steps
    check_kafka_connect || exit 1
    delete_existing_connector
    create_bronze_connector || exit 1
    verify_connector || exit 1
    show_connector_info

    echo ""
    echo "SUCCESS: Bronze connector deployment completed successfully!"
    echo ""
    echo "Next Steps:"
    echo "1. Start Kafka producer: python src/streaming/sales_event_producer.py"
    echo "2. Verify S3 Bronze ingestion"
    echo "3. Run Airflow DAG with Bronze audit trail"
    echo "4. Check Bronze → Silver → Gold data flow"
}

# Execute main function
main "$@"
