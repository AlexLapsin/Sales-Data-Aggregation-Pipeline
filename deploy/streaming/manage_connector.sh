#!/bin/bash
# manage_connector.sh
# Management script for Snowflake Kafka Connect connector

set -euo pipefail

# Detect if running in Docker network or host network
if curl -s --connect-timeout 2 http://kafka-connect:8083/ >/dev/null 2>&1; then
    CONNECT_URL="http://kafka-connect:8083"
elif curl -s --connect-timeout 2 http://localhost:8083/ >/dev/null 2>&1; then
    CONNECT_URL="http://localhost:8083"
else
    echo "ERROR: Kafka Connect is not accessible at kafka-connect:8083 or localhost:8083"
    echo "Start it with: docker-compose --profile kafka-local up -d"
    exit 1
fi
CONNECTOR_NAME="sales-events-file-sink"

usage() {
    echo "Usage: $0 {status|start|stop|restart|delete|logs|list}"
    echo ""
    echo "Commands:"
    echo "  status   - Show connector status"
    echo "  start    - Start/resume connector"
    echo "  stop     - Pause connector"
    echo "  restart  - Restart connector"
    echo "  delete   - Delete connector"
    echo "  logs     - Show connector logs"
    echo "  list     - List all connectors"
    exit 1
}

check_connect() {
    if ! curl -f "$CONNECT_URL/" >/dev/null 2>&1; then
        echo "ERROR: Kafka Connect is not running at $CONNECT_URL"
        echo "Start it with: docker-compose -f docker-compose-connect.yml up -d"
        exit 1
    fi
}

case "${1:-}" in
    status)
        check_connect
        echo "Connector Status:"
        curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.'
        ;;
    start)
        check_connect
        echo "Starting connector..."
        curl -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/resume"
        echo "SUCCESS: Connector started"
        ;;
    stop)
        check_connect
        echo "Stopping connector..."
        curl -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/pause"
        echo "SUCCESS: Connector stopped"
        ;;
    restart)
        check_connect
        echo "Restarting connector..."
        curl -X POST "$CONNECT_URL/connectors/$CONNECTOR_NAME/restart"
        echo "SUCCESS: Connector restarted"
        ;;
    delete)
        check_connect
        echo "Deleting connector..."
        curl -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
        echo "SUCCESS: Connector deleted"
        ;;
    logs)
        echo "Connector Logs:"
        docker logs kafka-connect --tail 50 -f
        ;;
    list)
        check_connect
        echo "All Connectors:"
        curl -s "$CONNECT_URL/connectors" | jq '.'
        ;;
    *)
        usage
        ;;
esac
