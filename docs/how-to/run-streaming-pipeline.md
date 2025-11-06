# Run Streaming Pipeline

Execute real-time Kafka streaming through Bronze, Silver, and Gold layers.

## Prerequisites

- Infrastructure deployed
- Environment configured
- Docker services running (including Kafka)
- Batch pipeline understanding (recommended)

## Pipeline Architecture

```
Kafka Producer → Kafka Broker → S3 Bronze → Spark Streaming → Delta Silver → dbt → Snowflake Gold
```

**Execution flow:**
1. Producer generates sales events to Kafka topic
2. Kafka Connect writes events to Bronze S3
3. Airflow DAG processes Bronze files with Spark
4. Spark writes to Silver Delta Lake
5. dbt transforms to Gold dimensional model

## Verify Kafka is Running

Check Kafka container status:

```bash
docker-compose ps kafka
```

Expected output: `Up` status

Test Kafka connectivity:

```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output: List of topics (initially empty or default topics)

## Create Kafka Topic

Create the sales events topic:

```bash
docker-compose exec kafka kafka-topics \
  --create \
  --topic sales_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Expected output:
```
Created topic sales_events.
```

Verify topic creation:

```bash
docker-compose exec kafka kafka-topics --describe --topic sales_events --bootstrap-server localhost:9092
```

Expected output:
```
Topic: sales_events     PartitionCount: 3       ReplicationFactor: 1
```

## Start Sales Event Producer

Run the Kafka producer to generate fake sales events:

```bash
python src/streaming/sales_event_producer.py --events 100 --interval 1
```

Options:
- `--events` - Number of events to generate (default: infinite)
- `--interval` - Seconds between events (default: 1)
- `--topic` - Kafka topic name (default: sales_events)

Expected output:
```
[2024-01-15 10:30:01] INFO - Connected to Kafka broker: localhost:9092
[2024-01-15 10:30:01] INFO - Producing to topic: sales_events
[2024-01-15 10:30:01] INFO - Event 1/100 produced - Order ID: ORD-123456
[2024-01-15 10:30:02] INFO - Event 2/100 produced - Order ID: ORD-123457
[2024-01-15 10:30:03] INFO - Event 3/100 produced - Order ID: ORD-123458
...
```

**Run in background:**

```bash
nohup python src/streaming/sales_event_producer.py --events 1000 --interval 1 > producer.log 2>&1 &
```

Monitor logs:
```bash
tail -f producer.log
```

## Verify Events in Kafka

Consume messages from the topic to verify:

```bash
docker-compose exec kafka kafka-console-consumer \
  --topic sales_events \
  --from-beginning \
  --bootstrap-server localhost:9092 \
  --max-messages 5
```

Expected output (JSON events):
```json
{"order_id": "ORD-123456", "order_date": "2024-01-15", "customer_id": "C001", "product_id": "P001", "sales": 1250.50, "quantity": 5, ...}
{"order_id": "ORD-123457", "order_date": "2024-01-15", "customer_id": "C002", "product_id": "P002", "sales": 875.25, "quantity": 3, ...}
...
```

## Configure Kafka Connect (S3 Sink)

Deploy the Kafka Connect S3 sink connector:

```bash
bash deploy/streaming/deploy_connector.sh
```

This configures Kafka Connect to:
- Read from `sales_events` topic
- Write to Bronze S3 in JSON format
- Partition by date (`year=YYYY/month=MM/day=DD`)

Verify connector deployment:

```bash
curl http://localhost:8083/connectors/s3-sink-sales/status
```

Expected output:
```json
{
  "name": "s3-sink-sales",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ]
}
```

## Verify Bronze Layer

Wait 1-2 minutes for Kafka Connect to flush events to S3.

Check Bronze S3 for streaming data:

```bash
aws s3 ls s3://your-project-raw-bucket/sales_data/sales_events/ --recursive
```

Expected structure:
```
sales_data/sales_events/
├── year=2024/
│   └── month=01/
│       └── day=15/
│           ├── sales_events+0+0000000000.json
│           ├── sales_events+1+0000000000.json
│           └── sales_events+2+0000000000.json
```

Files are named by Kafka partition and offset.

Download and inspect a file:

```bash
aws s3 cp s3://your-project-raw-bucket/sales_data/sales_events/year=2024/month=01/day=15/sales_events+0+0000000000.json -
```

Expected output: JSON events, one per line

## Enable Streaming Processing DAG

Access Airflow UI at http://localhost:8080.

Enable the DAG:
1. Find `streaming_processing_dag` in DAG list
2. Toggle the switch to enable
3. Verify schedule: Runs every 5 minutes

## Trigger DAG Manually

Option 1 - Via UI:
1. Click play button (▶) on `streaming_processing_dag`
2. Select "Trigger DAG"
3. Click "Trigger"

Option 2 - Via CLI:

```bash
docker-compose exec airflow-webserver airflow dags trigger streaming_processing_dag
```

## Monitor DAG Execution

Click on DAG name to view execution.

**Task sequence:**
1. `check_streaming_data` - Scans Bronze S3 for new event files
2. `process_streaming_data` - Executes Spark ETL to Silver
3. `trigger_analytics` - Triggers dbt transformation DAG

Execution time: 3-5 minutes

**Task Logs:**

Click tasks to view logs:
- `check_streaming_data`: List of detected JSON files
- `process_streaming_data`: Spark job progress, events processed
- `trigger_analytics`: DAG trigger confirmation

## Verify Silver Layer

Check Delta Lake files:

```bash
aws s3 ls s3://your-project-processed-bucket/silver/sales/ --recursive
```

Expected: Parquet files and `_delta_log/` with updated transaction logs

Query Silver data to verify streaming ingestion:

```python
from delta import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("s3://your-project-processed-bucket/silver/sales/")

# Filter for streaming data (recent orders)
streaming_df = df.filter(df.order_date == "2024-01-15")
print(f"Streaming records: {streaming_df.count()}")

# Show sample
streaming_df.show(5)
```

## Verify Gold Layer

Query Snowflake for updated Gold tables:

```sql
USE DATABASE SALES_DB;
USE SCHEMA PUBLIC;

-- Check for recent orders (from streaming)
SELECT
    order_id,
    order_date,
    customer_name,
    sales_amount,
    profit
FROM fact_sales
WHERE order_date >= CURRENT_DATE - 1
ORDER BY order_date DESC
LIMIT 10;

-- Verify record count increased
SELECT COUNT(*) AS total_records FROM fact_sales;
```

## Continuous Streaming

For continuous streaming, run producer indefinitely:

```bash
python src/streaming/sales_event_producer.py --interval 5
```

This generates one event every 5 seconds, continuously.

The streaming DAG processes new events every 5 minutes automatically.

**Monitor end-to-end flow:**
1. Producer logs: Events generated
2. Kafka topic: `kafka-console-consumer` (see events)
3. Bronze S3: New JSON files appear
4. Airflow: `streaming_processing_dag` runs every 5 minutes
5. Silver S3: Delta Lake updated
6. Snowflake: New records in `fact_sales`

## Streaming Configuration

Customize streaming in `airflow/dags/streaming_processing_dag.py`:

**Change schedule interval:**
```python
schedule_interval="*/5 * * * *",  # Every 5 minutes (default)
schedule_interval="*/1 * * * *",  # Every minute (high-frequency)
```

**Change Kafka Connect flush interval:**

Edit `deploy/streaming/kafka-connect-s3-sink.json`:
```json
{
  "flush.size": "1000",           // Write after 1000 records
  "rotate.interval.ms": "60000"   // Or write every 60 seconds
}
```

Redeploy connector:
```bash
bash deploy/streaming/deploy_connector.sh
```

## Performance Tuning

**High-throughput streaming (>1000 events/sec):**

1. Increase Kafka partitions:
```bash
docker-compose exec kafka kafka-topics \
  --alter \
  --topic sales_events \
  --partitions 10 \
  --bootstrap-server localhost:9092
```

2. Increase Kafka Connect tasks:
```json
{
  "tasks.max": "5"
}
```

3. Adjust Spark streaming batch size:
```python
df = spark.readStream \
    .option("maxFilesPerTrigger", 10) \
    .json(input_path)
```

**Low-latency requirements (<1 min):**

1. Reduce flush interval:
```json
{
  "flush.size": "100",
  "rotate.interval.ms": "30000"
}
```

2. Run DAG every minute:
```python
schedule_interval="*/1 * * * *"
```

## Troubleshooting

**Producer not connecting to Kafka:**
- Verify Kafka is running: `docker-compose ps kafka`
- Check Kafka logs: `docker-compose logs kafka`
- Test connectivity: `telnet localhost 9092`

**No events in Bronze S3:**
- Verify Kafka Connect is running: `curl http://localhost:8083/connectors`
- Check connector status: `curl http://localhost:8083/connectors/s3-sink-sales/status`
- Check connector logs: `docker-compose logs kafka-connect`
- Verify S3 permissions (IAM role has write access)

**Streaming DAG not processing data:**
- Check task logs in `check_streaming_data` task
- Verify Bronze S3 path matches: `sales_data/sales_events/year=YYYY/...`
- Check Spark logs in `process_streaming_data` task

**Duplicate records in Gold:**
- Verify Spark write mode is `append`, not `overwrite`
- Check Delta Lake transaction log for duplicate writes
- Review dbt deduplication logic in `stg_sales_silver.sql`

**Events missing in Snowflake:**
- Verify `analytics_dag` triggered successfully
- Check dbt logs for transformation errors
- Verify Snowflake connection in Airflow

## Stop Streaming

Stop producer:
```bash
# Find producer process
ps aux | grep sales_event_producer

# Kill process
kill <PID>
```

Stop Kafka Connect:
```bash
curl -X DELETE http://localhost:8083/connectors/s3-sink-sales
```

Pause streaming DAG in Airflow UI.

## Next Steps

- [Batch Pipeline](run-batch-pipeline.md) - Combine batch and streaming
- [Troubleshooting](troubleshooting.md) - Common issues
- [Architecture](../reference/architecture.md) - Streaming architecture details
