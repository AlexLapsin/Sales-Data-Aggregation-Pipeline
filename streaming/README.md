# Streaming Data Components

This directory contains the streaming data ingestion components for the sales data pipeline, including Kafka producers and connector configurations.

## Overview

The streaming pipeline simulates real-time sales events and ingests them into Snowflake via Kafka Connect. This setup supports both local development (Docker Kafka) and production deployment (AWS MSK).

## Components

### 1. Kafka Producer (`kafka_producer.py`)
- Generates realistic sales events with proper data distribution
- Supports both Docker Kafka and AWS MSK
- Configurable event rate and batch processing
- Production-ready with proper error handling and monitoring

### 2. Configuration (`producer_config.json`)
- Environment-specific configurations
- Bootstrap servers, topics, and performance tuning
- Sample usage commands

### 3. Test Suite (`test_producer.py`)
- Data generation validation
- Kafka connectivity testing
- Quick verification of producer functionality

## Quick Start

### Local Development (Docker Kafka)

1. **Start Kafka services:**
   ```bash
   # From project root
   docker-compose --profile kafka-local up -d
   ```

2. **Install dependencies:**
   ```bash
   cd streaming
   pip install -r requirements.txt
   ```

3. **Test the producer:**
   ```bash
   python test_producer.py
   ```

4. **Run continuous producer:**
   ```bash
   # Send 1000 events at 2-second intervals
   python kafka_producer.py --count 1000 --interval 2.0 --verbose

   # Burst mode for load testing
   python kafka_producer.py --burst --count 5000
   ```

### AWS MSK Deployment

1. **Deploy MSK infrastructure:**
   ```bash
   cd ../infra
   export ENABLE_MSK=true
   terraform apply
   ```

2. **Get bootstrap servers:**
   ```bash
   terraform output kafka_bootstrap_servers
   ```

3. **Update configuration:**
   ```bash
   export KAFKA_BOOTSTRAP_SERVERS="your-msk-bootstrap-servers"
   python kafka_producer.py --brokers $KAFKA_BOOTSTRAP_SERVERS
   ```

## Usage Examples

### Basic Usage
```bash
# Default: Send events to localhost:9092 every 1 second
python kafka_producer.py

# Custom broker and topic
python kafka_producer.py --brokers "broker1:9092,broker2:9092" --topic "my_sales"

# Limited run with faster rate
python kafka_producer.py --count 500 --interval 0.5
```

### Configuration File
```bash
# Use predefined configuration
python kafka_producer.py --config producer_config.json

# Override specific settings
python kafka_producer.py --config producer_config.json --count 1000
```

### Monitoring
```bash
# Verbose output for debugging
python kafka_producer.py --verbose

# Check Kafka UI (if using Docker)
open http://localhost:8090
```

## Data Schema

Each sales event contains the following fields:

```json
{
  "order_id": "uuid",
  "store_id": "STORE_LOCATION_NAME",
  "product_id": "CATEGORY_PRODUCT_ID",
  "product_name": "Product Name",
  "category": "Product Category",
  "quantity": 1,
  "unit_price": 25.99,
  "total_price": 25.99,
  "sale_timestamp": "2024-01-15T10:30:00Z",
  "customer_id": "CUST_12345",
  "payment_method": "Credit Card",
  "discount": 0.1,
  "store_location": "New York"
}
```

## Performance Tuning

### Producer Configuration
- **Batch size**: 16KB for better throughput
- **Compression**: Snappy compression enabled
- **Acknowledgments**: `acks=all` for durability
- **Retries**: 3 retries with exponential backoff

### Rate Control
```bash
# Slow rate for real-time simulation
python kafka_producer.py --interval 2.0

# High rate for load testing
python kafka_producer.py --interval 0.1

# Maximum throughput
python kafka_producer.py --burst
```

## Troubleshooting

### Common Issues

1. **Connection refused**
   ```bash
   # Check if Kafka is running
   docker-compose --profile kafka-local ps

   # Check logs
   docker-compose --profile kafka-local logs kafka
   ```

2. **Topic not found**
   ```bash
   # Create topic manually (auto-creation is enabled by default)
   docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic sales_events
   ```

3. **MSK connection issues**
   ```bash
   # Verify security groups allow port 9092-9094
   # Check MSK cluster status in AWS console
   # Ensure IAM permissions for MSK access
   ```

### Monitoring Commands

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-group

# Consume messages for testing
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sales_events --from-beginning
```

## Next Steps

1. **Snowflake Connector**: Configure Kafka Connect sink to load data into Snowflake
2. **Schema Registry**: Add Avro/JSON schema validation
3. **Monitoring**: Implement Prometheus metrics collection
4. **Scaling**: Configure multiple producer instances for higher throughput

## Configuration Reference

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: Comma-separated broker list
- `KAFKA_TOPIC`: Target topic name (default: sales_events)
- `PRODUCER_INTERVAL`: Seconds between events
- `LOG_LEVEL`: DEBUG, INFO, WARNING, ERROR

### Docker Compose Profiles
- `kafka-local`: Start local Kafka cluster with UI
- Default profile: Only Airflow and existing services

## Snowflake Kafka Connect

### Quick Setup
```bash
# 1. Start Kafka Connect with Snowflake connector
docker-compose -f docker-compose-connect.yml up -d

# 2. Configure your Snowflake credentials in .env
# 3. Deploy the connector
./deploy_connector.sh

# 4. Test the pipeline
python kafka_producer.py --count 20
```

### Connector Management
```bash
# Check connector status
./manage_connector.sh status

# Pause/resume connector
./manage_connector.sh stop
./manage_connector.sh start

# View logs
./manage_connector.sh logs
```

### Connector Configurations
- `snowflake_connector.json` - Private key authentication (production)
- `snowflake_connector_password.json` - Password authentication (development)
