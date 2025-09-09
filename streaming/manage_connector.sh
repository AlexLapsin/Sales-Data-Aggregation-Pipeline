#!/bin/bash
# manage_connector.sh
# Management script for Snowflake Kafka Connect connector

set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="sales-events-snowflake-sink"

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
        echo "❌ Kafka Connect is not running at $CONNECT_URL"
        echo "Start it with: docker-compose -f docker-compose-connect.yml up -d"
        exit 1
    fi
}

case "${1:-}" in
    status)
        check_connect
        echo "📊 Connector Status:"
        curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq '.'
        ;;
    start)
        check_connect
        echo "▶️  Starting connector..."
        curl -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/resume"
        echo "✅ Connector started"
        ;;
    stop)
        check_connect
        echo "⏸️  Stopping connector..."
        curl -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/pause"
        echo "✅ Connector stopped"
        ;;
    restart)
        check_connect
        echo "🔄 Restarting connector..."
        curl -X POST "$CONNECT_URL/connectors/$CONNECTOR_NAME/restart"
        echo "✅ Connector restarted"
        ;;
    delete)
        check_connect
        echo "🗑️  Deleting connector..."
        curl -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
        echo "✅ Connector deleted"
        ;;
    logs)
        echo "📋 Connector Logs:"
        docker logs kafka-connect --tail 50 -f
        ;;
    list)
        check_connect
        echo "📋 All Connectors:"
        curl -s "$CONNECT_URL/connectors" | jq '.'
        ;;
    *)
        usage
        ;;
esac
