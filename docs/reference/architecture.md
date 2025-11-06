# Architecture Reference

Technical specification of the sales data pipeline system design.

## System Overview

Enterprise data pipeline implementing Medallion architecture with batch and streaming ingestion.

**Technology Stack:**
- **Orchestration**: Apache Airflow 2.7+
- **Batch Processing**: Apache Spark 3.4+ (PySpark)
- **Streaming**: Apache Kafka 3.5+
- **Storage**: AWS S3, Delta Lake
- **Transformation**: dbt 1.6+
- **Warehouse**: Snowflake
- **Infrastructure**: Terraform, Docker

## Medallion Architecture

Three-layer data architecture for quality and performance:

**Bronze Layer (Raw):**
- Purpose: Immutable audit trail of source data
- Format: CSV (batch), JSON (streaming)
- Storage: AWS S3 with date partitioning
- Retention: 90 days (configurable via lifecycle policies)
- Schema: Original source schema preserved

**Silver Layer (Refined):**
- Purpose: Cleansed, validated, deduplicated data
- Format: Delta Lake (Parquet + transaction log)
- Storage: AWS S3
- Retention: 180 days active, then Glacier
- Schema: Unified 24-field canonical schema
- Features: ACID transactions, schema evolution, time travel

**Gold Layer (Analytics):**
- Purpose: Business-ready dimensional model
- Format: Snowflake tables
- Storage: Snowflake managed storage
- Schema: Star schema (facts and dimensions)
- Optimization: Clustering keys, materialized views

## Data Flow Architecture

### Batch Pipeline

```
CSV Files
    ↓
[Data Uploader] → S3 Bronze (sales_data/year=YYYY/month=MM/day=DD/)
    ↓
[Airflow Sensor] → Detects new files every 15 minutes
    ↓
[Spark ETL] → Reads Bronze CSV, validates, deduplicates
    ↓
[Delta Writer] → S3 Silver (Delta Lake format)
    ↓
[Dataset Trigger] → Triggers Analytics DAG
    ↓
[dbt Run] → Transforms Silver to Gold dimensional model
    ↓
Snowflake Tables (fact_sales, dim_customer, dim_product, dim_date)
```

### Streaming Pipeline

```
Kafka Producer (sales_event_producer.py)
    ↓
Kafka Broker (topic: sales_events, partitions: 3)
    ↓
[Kafka Connect S3 Sink] → S3 Bronze (sales_data/sales_events/year=YYYY/...)
    ↓
[Airflow Sensor] → Detects new JSON files every 5 minutes
    ↓
[Spark Streaming] → Reads Bronze JSON, validates, deduplicates
    ↓
[Delta Merge] → S3 Silver (upsert to Delta Lake)
    ↓
[Dataset Trigger] → Triggers Analytics DAG
    ↓
[dbt Run] → Incremental models update Gold layer
    ↓
Snowflake Tables (updated with streaming data)
```

## Component Architecture

### Airflow Orchestration

**Service Components:**
- `airflow-webserver`: UI and REST API (port 8080)
- `airflow-scheduler`: DAG parsing and task scheduling
- `airflow-triggerer`: Event-driven task execution (Dataset triggers)
- `postgres`: Metadata database (DAG runs, task instances, variables)
- `redis`: Celery backend for task queueing (if using CeleryExecutor)

**DAG Architecture:**

**batch_processing_dag.py:**
- Schedule: Every 15 minutes (`*/15 * * * *`)
- Trigger: Time-based (cron)
- Tasks:
  1. `check_new_files`: S3FileSensor for new CSV files
  2. `process_batch_data`: SparkSubmitOperator (batch_etl.py)
  3. `trigger_analytics`: TriggerDagRunOperator
- Outlets: `silver_batch_dataset` (triggers analytics_dag)

**streaming_processing_dag.py:**
- Schedule: Every 5 minutes (`*/5 * * * *`)
- Trigger: Time-based (cron)
- Tasks:
  1. `check_streaming_data`: S3KeySensor for new JSON files
  2. `process_streaming_data`: SparkSubmitOperator (streaming_etl.py)
  3. `trigger_analytics`: TriggerDagRunOperator
- Outlets: `silver_streaming_dataset` (triggers analytics_dag)

**analytics_dag.py:**
- Schedule: Dataset-driven (no cron)
- Trigger: When `silver_batch_dataset` or `silver_streaming_dataset` updated
- Tasks:
  1. `dbt_test`: Data quality tests on Silver layer
  2. `dbt_run`: Execute dimensional model transformations
  3. `dbt_test_marts`: Data quality tests on Gold layer

**kafka_connector_monitor_dag.py:**
- Schedule: Every 1 minute (`*/1 * * * *`)
- Trigger: Time-based (cron)
- Tasks:
  1. `check_connector_status`: HTTP request to Kafka Connect API
  2. `alert_on_failure`: Send alert if connector failed

### Spark Processing

**Execution Modes:**

**Databricks Mode (Production):**
- Trigger: When `DATABRICKS_HOST` and `DATABRICKS_TOKEN` set in `.env`
- Submit: REST API to Databricks Jobs API
- Cluster: Auto-scaling job cluster (2-8 workers)
- Libraries: Delta Lake, Snowflake Spark Connector
- Advantages: Scalable, managed, production-grade

**Local Mode (Development):**
- Trigger: When Databricks credentials not configured
- Submit: Local Spark master (`local[*]`)
- Resources: Docker container limits
- Advantages: Fast iteration, no cloud costs

**Spark Jobs:**

**src/spark/jobs/batch_etl.py:**
- Input: S3 Bronze CSV files (date-partitioned)
- Processing:
  1. Read CSV with schema inference
  2. Validate 24-field canonical schema
  3. Data quality checks (nulls, duplicates, ranges)
  4. Standardize date formats and field names
  5. Deduplicate by order_id (keep latest)
- Output: Delta Lake Silver layer (append mode)
- Configuration:
  ```python
  spark.executor.memory: 4g
  spark.driver.memory: 2g
  spark.sql.shuffle.partitions: 200
  ```

**src/spark/jobs/streaming_etl.py:**
- Input: S3 Bronze JSON files (Kafka Connect output)
- Processing:
  1. Read JSON with schema validation
  2. Parse nested structures
  3. Convert to 24-field canonical schema
  4. Deduplicate and merge with existing Delta table
- Output: Delta Lake Silver layer (merge mode)
- Configuration:
  ```python
  mergeSchema: true
  optimizeWrite: true
  ```

### Delta Lake Storage

**Transaction Log Architecture:**

```
silver/sales/
├── _delta_log/
│   ├── 00000000000000000000.json     # Initial commit
│   ├── 00000000000000000001.json     # Commit 2
│   ├── 00000000000000000002.json     # Commit 3
│   └── _last_checkpoint                # Checkpoint metadata
├── part-00000-xxx.snappy.parquet      # Data files
├── part-00001-xxx.snappy.parquet
└── part-00002-xxx.snappy.parquet
```

**Transaction Log Contents:**
- Add/Remove operations for Parquet files
- Schema evolution history
- Table metadata (partitioning, properties)
- Transaction timestamps and IDs

**ACID Guarantees:**
- **Atomicity**: All-or-nothing commits
- **Consistency**: Schema enforcement
- **Isolation**: Serializable isolation level
- **Durability**: S3 persistence with replication

### dbt Transformations

**Project Structure:**

```
dbt/
├── models/
│   ├── staging/
│   │   ├── _sources.yml              # Silver layer source definitions
│   │   ├── stg_sales_silver.sql      # Staging model from Silver
│   │   ├── stg_customers_from_sales.sql
│   │   └── stg_products_from_sales.sql
│   ├── intermediate/
│   │   ├── int_sales_unified.sql     # Unified batch + streaming
│   │   ├── int_product_scd2.sql      # SCD Type 2 product history
│   │   └── int_store_scd2.sql        # SCD Type 2 store history
│   └── marts/
│       ├── fact_sales.sql            # Sales fact table
│       ├── dim_customer.sql          # Customer dimension
│       ├── dim_product.sql           # Product dimension
│       └── dim_date.sql              # Date dimension
├── tests/
│   └── singular/
│       ├── test_business_logic_validation.sql
│       ├── test_dimensional_model_integrity.sql
│       └── test_scd2_integrity.sql
└── macros/
    └── data_quality.sql              # Reusable quality check macros
```

**Model Materialization:**
- **Staging**: Views (ephemeral, no storage cost)
- **Intermediate**: Tables (incremental where applicable)
- **Marts**: Tables with clustering keys

**Incremental Strategy:**
```sql
-- fact_sales.sql
{{ config(
    materialized='incremental',
    unique_key='sales_key',
    cluster_by=['order_date']
) }}

{% if is_incremental() %}
  WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

### Kafka Streaming

**Architecture:**

```
Producer (Python)
    ↓
Kafka Broker
    ├── Topic: sales_events
    ├── Partitions: 3 (for parallelism)
    └── Replication: 1 (single-node development)
    ↓
Kafka Connect (S3 Sink Connector)
    ├── Flush Size: 1000 records
    ├── Rotate Interval: 60 seconds
    ├── Format: JSON (one record per line)
    └── Partitioner: TimeBasedPartitioner (hourly)
    ↓
S3 Bronze (sales_data/sales_events/year=YYYY/month=MM/day=DD/hour=HH/)
```

**Producer Configuration:**
- `acks=all`: Wait for all replicas to acknowledge
- `compression.type=snappy`: Compress messages
- `batch.size=16384`: Batch size in bytes
- `linger.ms=10`: Wait up to 10ms for batching

**Connector Configuration:**
- `tasks.max=3`: Parallel tasks (matches partition count)
- `s3.part.size=5242880`: 5MB part size for multipart upload
- `flush.size=1000`: Write after 1000 records
- `rotate.interval.ms=60000`: Or write every 60 seconds

## Snowflake Integration

### Authentication

**Key-Pair Authentication (Production):**

The pipeline uses RSA key-pair authentication for enhanced security:

1. Generate RSA key pair:
```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

2. Register public key in Snowflake:
```sql
ALTER USER your_username SET RSA_PUBLIC_KEY='YOUR_PUBLIC_KEY_HERE';
```

3. Configure environment:
```bash
SNOWFLAKE_PRIVATE_KEY_PATH=/config/keys/rsa_key.p8
SNOWFLAKE_KEY_PASSPHRASE=YOUR_PASSPHRASE_HERE
```

**Connection Implementation:**

Terraform provider:
```hcl
provider "snowflake" {
  account_name           = var.SNOWFLAKE_ACCOUNT_NAME
  organization_name      = var.SNOWFLAKE_ORGANIZATION_NAME
  user                   = var.SNOWFLAKE_USER
  private_key            = file(var.SNOWFLAKE_PRIVATE_KEY_PATH)
  private_key_passphrase = var.SNOWFLAKE_KEY_PASSPHRASE
  role                   = var.SNOWFLAKE_ROLE
}
```

Python connection (analytics_dag.py):
```python
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# Load encrypted private key
with open('/config/keys/rsa_key.p8', 'rb') as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=os.getenv('SNOWFLAKE_KEY_PASSPHRASE').encode(),
        backend=default_backend()
    )

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    private_key=pkb,
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role=os.getenv('SNOWFLAKE_ROLE')
)
```

### Storage Integration

**S3 External Stage:**

Terraform creates storage integration for S3 access:
```sql
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-project-processed-bucket/silver/');
```

**IAM Trust Policy:**

Snowflake assumes IAM role to access S3:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::YOUR_ACCOUNT_ID:user/YOUR_SNOWFLAKE_USER"
    },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "sts:ExternalId": "YOUR_SNOWFLAKE_EXTERNAL_ID"
      }
    }
  }]
}
```

**Delta Lake Direct Access:**

Snowflake queries Delta Lake files in S3 without data movement:

```sql
-- Create external volume (Snowflake Delta Direct)
CREATE EXTERNAL VOLUME delta_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'silver_layer'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://your-project-processed-bucket/silver/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-access-role'
    )
  );

-- Create Delta table reference
CREATE OR REPLACE ICEBERG TABLE sales_silver
  EXTERNAL_VOLUME = 'delta_volume'
  CATALOG = 'SNOWFLAKE'
  METADATA$DELTA_LOCATION = 's3://your-project-processed-bucket/silver/sales/';

-- Query Delta Lake directly
SELECT * FROM sales_silver WHERE order_date >= '2024-01-01';
```

Benefits:
- Zero data duplication (data stays in S3)
- Reduced storage costs
- Automatic schema synchronization
- Real-time access to Silver layer updates

## Infrastructure

**Terraform Modules:**

**modules/s3:**
- Raw bucket (Bronze layer)
- Processed bucket (Silver layer)
- Bucket policies for Snowflake access
- Lifecycle policies (90-day Bronze, 180-day Silver)

**modules/iam:**
- Snowflake S3 access role
- Read/write policies for S3 buckets
- Trust policy with Snowflake external ID

**modules/snowflake:**
- Database and schema creation
- Compute warehouse configuration
- Storage integration setup
- External volume for Delta Direct
- User and role grants

**Docker Services:**

**docker-compose.yml:**
- postgres: Airflow metadata (port 5432)
- redis: Task queue backend (port 6379)
- airflow-webserver: UI (port 8080)
- airflow-scheduler: DAG scheduler
- airflow-triggerer: Dataset triggers
- zookeeper: Kafka coordination (port 2181)
- kafka: Message broker (port 9092)

**Resource Allocation:**
- postgres: 512MB memory
- redis: 256MB memory
- airflow-webserver: 2GB memory
- airflow-scheduler: 1GB memory
- kafka: 2GB memory
- zookeeper: 512MB memory

## Security Architecture

**Authentication:**
- Snowflake: RSA key-pair authentication (2048-bit keys)
- AWS: IAM access keys with least privilege policies
- Databricks: Personal access tokens (PAT)
- Airflow: Username/password (configurable for LDAP/OAuth)

**Secrets Management:**
- Development: Environment variables in `.env` (gitignored)
- Airflow: Variables and Connections (encrypted in metadata DB)
- Production: AWS Secrets Manager
- Databricks: Secret scopes for credentials

**Data Encryption:**
- In-transit: TLS 1.2+ for all connections (S3, Snowflake, Kafka)
- At-rest: S3 server-side encryption (SSE-S3 or SSE-KMS)
- At-rest: Snowflake automatic encryption
- Kafka: SSL/SASL for production deployments

**Access Control:**
- AWS: IAM roles with least privilege
- Snowflake: RBAC (role-based access control)
- S3: Bucket policies restricting access to Snowflake IAM user
- Airflow: RBAC for DAG permissions

**Network Security:**
- Snowflake: Network policies for IP whitelisting
- AWS: Security groups and VPC (production)
- Kafka: SASL authentication (production)
- Private key protection: Encrypted with passphrase, never committed to git

## Monitoring and Observability

**Airflow Metrics:**
- Task success/failure rates
- DAG execution duration
- Task queue depth
- Scheduler heartbeat

**Data Quality Metrics:**
- Record counts by layer (Bronze/Silver/Gold)
- Schema validation failures
- Duplicate detection rate
- Data freshness (max lag between layers)

**Infrastructure Metrics:**
- Docker container health
- Spark job execution time
- Kafka consumer lag
- Snowflake warehouse credit usage

## Scalability Considerations

**Horizontal Scaling:**
- Kafka: Add partitions and brokers
- Spark: Increase Databricks cluster size
- Airflow: Add worker nodes (CeleryExecutor)
- Snowflake: Scale warehouse size (X-Small to 6X-Large)

**Vertical Scaling:**
- Docker: Increase memory allocations
- Spark: Increase executor/driver memory
- Airflow: Increase parallelism settings

**Partitioning Strategy:**
- Bronze: Date partitioning (year/month/day)
- Silver: Delta Lake automatic file management
- Gold: Snowflake clustering keys (order_date, customer_id)

## Disaster Recovery

**Backup Strategy:**
- Bronze: S3 versioning enabled, 90-day retention
- Silver: Delta Lake time travel (30 days)
- Gold: Snowflake Time Travel (90 days with Enterprise edition)

**Recovery Procedures:**
- Bronze corruption: Restore from S3 versioning
- Silver corruption: Rebuild from Bronze layer
- Gold corruption: Restore from Snowflake Time Travel or rebuild from Silver

**High Availability:**
- S3: Multi-AZ replication (AWS default)
- Snowflake: Multi-cluster warehouse (production)
- Airflow: Multi-scheduler (v2.7+)
- Kafka: Multi-broker with replication (production)
