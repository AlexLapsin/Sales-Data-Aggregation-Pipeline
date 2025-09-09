#!/bin/bash
# deploy_connector.sh
# Deploy Snowflake Kafka Connect connector

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="sales-events-snowflake-sink"

echo "🔌 Deploying Snowflake Kafka Connect Connector"
echo "=============================================="

# Function to check if Kafka Connect is ready
wait_for_connect() {
    echo "⏳ Waiting for Kafka Connect to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -f "$CONNECT_URL/" >/dev/null 2>&1; then
            echo "✅ Kafka Connect is ready"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts - waiting for Kafka Connect..."
        sleep 10
        ((attempt++))
    done

    echo "❌ Kafka Connect failed to start after $max_attempts attempts"
    return 1
}

# Function to substitute environment variables in JSON
substitute_env_vars() {
    local template_file="$1"
    local output_file="$2"

    envsubst < "$template_file" > "$output_file"
    echo "✅ Environment variables substituted in $output_file"
}

# Load environment variables
if [[ -f "$SCRIPT_DIR/../.env" ]]; then
    echo "📋 Loading environment variables..."
    set -a
    source "$SCRIPT_DIR/../.env"
    set +a
else
    echo "⚠️  .env file not found, using defaults"
fi

# Set default values if not provided
export SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-YOUR_ACCOUNT.us-east-1.aws}"
export SNOWFLAKE_USER="${SNOWFLAKE_USER:-YOUR_USERNAME}"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-YOUR_SNOWFLAKE_PASSWORD}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-SALES_DW}"
export SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-RAW}"
export SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-SYSADMIN}"

# Check if Kafka Connect is running
if ! wait_for_connect; then
    echo "❌ Please start Kafka Connect first:"
    echo "   docker-compose -f docker-compose-connect.yml up -d"
    exit 1
fi

# Choose connector configuration
CONNECTOR_CONFIG="snowflake_connector_password.json"
if [[ "${1:-}" == "--private-key" ]]; then
    CONNECTOR_CONFIG="snowflake_connector.json"
    echo "🔑 Using private key authentication"
else
    echo "🔐 Using password authentication"
fi

# Substitute environment variables
TEMP_CONFIG="/tmp/connector_config.json"
substitute_env_vars "$SCRIPT_DIR/$CONNECTOR_CONFIG" "$TEMP_CONFIG"

# Check if connector already exists
if curl -f "$CONNECT_URL/connectors/$CONNECTOR_NAME" >/dev/null 2>&1; then
    echo "🔄 Connector $CONNECTOR_NAME already exists, updating..."

    # Update existing connector
    curl -X PUT \
        -H "Content-Type: application/json" \
        --data @"$TEMP_CONFIG" \
        "$CONNECT_URL/connectors/$CONNECTOR_NAME/config"

    echo "✅ Connector updated successfully"
else
    echo "🆕 Creating new connector..."

    # Create new connector
    curl -X POST \
        -H "Content-Type: application/json" \
        --data @"$TEMP_CONFIG" \
        "$CONNECT_URL/connectors"

    echo "✅ Connector created successfully"
fi

# Clean up temporary file
rm -f "$TEMP_CONFIG"

# Check connector status
echo ""
echo "📊 Connector Status:"
curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.'

echo ""
echo "🎉 Snowflake Kafka Connector deployed!"
echo ""
echo "Next steps:"
echo "1. Check connector status: curl $CONNECT_URL/connectors/$CONNECTOR_NAME/status"
echo "2. View connector logs: docker logs kafka-connect"
echo "3. Produce test messages: cd .. && python streaming/kafka_producer.py --count 10"
echo "4. Check Snowflake table: SELECT * FROM SALES_DW.RAW.SALES_RAW LIMIT 10;"
