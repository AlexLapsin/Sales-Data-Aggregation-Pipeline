# Getting Started Tutorial

Complete walkthrough from zero to running pipeline. Total time: 60 minutes.

## Prerequisites

Before starting, you need:

**Cloud Accounts:**
- AWS account with admin access
- Snowflake trial account (30-day free trial, no credit card required)

**Local Tools:**
- Docker Desktop (20.10+) and Docker Compose (2.0+)
- Git (2.30+)
- Terraform (1.0+)
- AWS CLI (2.0+) configured with credentials
- Python 3.9+ (for validation scripts)

**Knowledge:**
- Basic command line usage
- Basic understanding of data pipelines
- Basic SQL knowledge

## Step 1: Clone Repository (2 min)

Clone the project and navigate to the directory:

```bash
git clone https://github.com/your-org/sales_data_aggregation_pipeline.git
cd sales_data_aggregation_pipeline
```

Verify project structure:

```bash
ls -la
```

Expected output: `airflow/`, `dbt/`, `infrastructure/`, `src/`, `.env.example`, `docker-compose.yml`

## Step 2: Configure Environment (5 min)

Copy the environment template:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1

# S3 Buckets (will be created by Terraform)
RAW_BUCKET=your-project-raw-bucket
PROCESSED_BUCKET=your-project-processed-bucket

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SALES_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Project Settings
PROJECT_NAME=sales-pipeline
ENVIRONMENT=dev
```

Verify configuration:

```bash
python tools/validation/config_validator.py --validate-all
```

Expected output: `All validations passed`

## Step 3: Deploy Infrastructure (15 min)

Export environment variables for Terraform:

```bash
source export_tf_vars.sh
```

Navigate to Terraform directory and initialize:

```bash
cd infrastructure/terraform
terraform init
```

Expected output: `Terraform has been successfully initialized`

Review the planned infrastructure:

```bash
terraform plan
```

Review the output to verify:
- 2 S3 buckets (raw and processed)
- IAM roles and policies
- Snowflake database and schema

Deploy the infrastructure:

```bash
terraform apply
```

Type `yes` when prompted. Wait 5-10 minutes for deployment.

Expected output: `Apply complete! Resources: X added, 0 changed, 0 destroyed`

Note the outputs:
- `raw_bucket_name`
- `processed_bucket_name`
- `snowflake_database`

Return to project root:

```bash
cd ../..
```

## Step 4: Upload Sample Data (3 min)

The project includes sample sales data. Upload it to the Bronze layer:

```bash
python src/bronze/data_uploader.py --source data/sample_sales.csv
```

Expected output: `Successfully uploaded to s3://your-project-raw-bucket/sales_data/year=2024/...`

Verify upload:

```bash
aws s3 ls s3://your-project-raw-bucket/sales_data/ --recursive
```

Expected output: List of partitioned CSV files

## Step 5: Start Services (10 min)

Start all services with Docker Compose:

```bash
docker-compose up --build
```

This starts:
- Apache Airflow (scheduler, webserver, triggerer)
- Kafka (broker and Zookeeper)
- PostgreSQL (metadata database)

Wait for initialization. Look for these log messages:
- `airflow-webserver | [timestamp] INFO - Listening at: http://0.0.0.0:8080`
- `kafka | [timestamp] INFO [KafkaServer id=1] started (kafka.server.KafkaServer)`

Services are ready when you see no new log messages for 30 seconds.

Press `Ctrl+C` to stop logs, then run in background:

```bash
docker-compose up -d
```

Verify all services are healthy:

```bash
docker-compose ps
```

Expected output: All services showing `Up` status

## Step 6: Access Airflow UI (2 min)

Open browser and navigate to:

```
http://localhost:8080
```

Login credentials:
- Username: `admin`
- Password: `admin`

You should see the Airflow dashboard with these DAGs:
- `batch_processing_dag` - CSV ingestion to Silver layer
- `streaming_processing_dag` - Kafka events to Silver layer
- `analytics_dag` - dbt transformations to Gold layer
- `kafka_connector_monitor_dag` - Monitor Kafka Connect

## Step 7: Run Batch Pipeline (10 min)

Enable and trigger the batch processing DAG:

1. In Airflow UI, find `batch_processing_dag`
2. Toggle the switch to enable the DAG
3. Click the play button (▶) and select "Trigger DAG"

Monitor the DAG execution:
- Click on the DAG name to view the graph
- Tasks should turn green as they complete
- Total execution time: 5-8 minutes

Expected task sequence:
1. `check_new_files` - Detects CSV files in Bronze S3
2. `process_batch_data` - Spark ETL to Silver Delta Lake
3. `trigger_analytics` - Triggers dbt transformations

Verify Silver layer output:

```bash
aws s3 ls s3://your-project-processed-bucket/silver/sales/ --recursive
```

Expected output: Delta Lake files (Parquet + transaction log)

## Step 8: Run Streaming Pipeline (10 min)

Start the Kafka producer to generate fake sales events:

```bash
python src/streaming/sales_event_producer.py --events 100 --interval 1
```

This generates 100 sales events, one per second. Expected output:

```
[2024-01-15 10:30:01] INFO - Produced event 1/100
[2024-01-15 10:30:02] INFO - Produced event 2/100
...
```

In Airflow UI, enable `streaming_processing_dag`. It runs every 5 minutes automatically.

Wait for the next scheduled run or manually trigger it.

Monitor execution (3-5 minutes):
- Tasks: `check_streaming_data` → `process_streaming_data` → `trigger_analytics`

Verify streaming data in Silver layer:

```bash
aws s3 ls s3://your-project-processed-bucket/silver/sales/ --recursive | grep sales_events
```

Expected output: More Delta Lake files from streaming ingestion

## Step 9: Run dbt Transformations (5 min)

The `analytics_dag` is triggered automatically after batch or streaming processing completes.

In Airflow UI, find the most recent `analytics_dag` run. Monitor execution:

1. `dbt_test` - Validates data quality in Silver layer
2. `dbt_run` - Executes transformations to Gold layer
3. `dbt_test_marts` - Validates Gold layer dimensional model

Expected output in Snowflake:
- `dim_customer` - Customer dimension table
- `dim_product` - Product dimension table
- `dim_date` - Date dimension table
- `fact_sales` - Sales fact table

Verify in Snowflake UI or CLI:

```sql
USE DATABASE SALES_DB;
USE SCHEMA PUBLIC;

SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM dim_date;
```

Expected output: Row counts matching your sample data volume

## Step 10: Verify End-to-End Flow (3 min)

Run this validation script to verify the complete pipeline:

```bash
python tools/validation/setup_doctor.py
```

Expected output:

```
Environment Configuration: PASS
AWS Connectivity: PASS
Snowflake Connectivity: PASS
S3 Buckets: PASS
Docker Services: PASS
Airflow DAGs: PASS (4/4 DAGs healthy)
Bronze Layer: PASS (files found)
Silver Layer: PASS (Delta Lake valid)
Gold Layer: PASS (tables found)

All checks passed. Pipeline is healthy.
```

## What You've Accomplished

You now have a fully operational data pipeline:

**Bronze Layer:**
- Raw CSV files stored in S3 with audit trail
- Partitioned by date for efficient queries

**Silver Layer:**
- Delta Lake format with ACID transactions
- Unified sales data from batch and streaming sources
- Schema enforcement and data quality checks

**Gold Layer:**
- Star schema dimensional model in Snowflake
- Fact and dimension tables optimized for analytics
- Ready for BI tool connections (Tableau, Power BI, etc.)

**Orchestration:**
- Automated batch processing via Airflow
- Real-time streaming with Kafka
- dbt transformations triggered automatically

## Next Steps

**Learn More:**
- [Run Batch Pipeline](../how-to/run-batch-pipeline.md) - Detailed batch processing guide
- [Run Streaming Pipeline](../how-to/run-streaming-pipeline.md) - Kafka streaming details
- [Architecture](../reference/architecture.md) - Deep dive into system design

**Customize:**
- Add your own CSV files to `data/` directory
- Modify dbt models in `dbt/models/`
- Adjust Airflow schedules in `airflow/dags/`

**Troubleshooting:**
- [Troubleshooting Guide](../how-to/troubleshooting.md) - Common issues and solutions
- Check service logs: `docker-compose logs [service-name]`
- Validate configuration: `python tools/validation/config_validator.py`
