# CLI Reference

Command-line tools and script usage for the pipeline.

## Data Upload

### data_uploader.py

Upload CSV files to Bronze S3 layer with date partitioning.

**Location:** `src/bronze/data_uploader.py`

**Usage:**

```bash
python src/bronze/data_uploader.py --source PATH [OPTIONS]
```

**Arguments:**

`--source PATH` (required)
- Path to CSV file or directory
- Supports single file or directory with multiple CSVs
- Example: `data/sample_sales.csv` or `data/csv_files/`

`--partition-date DATE` (optional)
- Custom partition date for upload
- Format: YYYY-MM-DD
- Default: Current date
- Example: `--partition-date 2024-01-15`

`--bucket BUCKET_NAME` (optional)
- Override default RAW_BUCKET from .env
- Example: `--bucket custom-raw-bucket`

`--recursive` (optional)
- Recursively upload files from subdirectories
- Only applies when source is directory

**Examples:**

```bash
# Upload single file
python src/bronze/data_uploader.py --source data/sample_sales.csv

# Upload with custom date
python src/bronze/data_uploader.py --source data/sales_2024_01.csv --partition-date 2024-01-15

# Upload directory recursively
python src/bronze/data_uploader.py --source data/csv_files/ --recursive

# Override bucket
python src/bronze/data_uploader.py --source data/sales.csv --bucket test-raw-bucket
```

**Output:**

```
Loading CSV: data/sample_sales.csv
Validating schema... PASS
Partitioning by date: 2024-01-15
Uploading to s3://your-raw-bucket/sales_data/year=2024/month=01/day=15/
Successfully uploaded 1 file(s)
```

## Kafka Streaming

### sales_event_producer.py

Generate and send sales events to Kafka topic.

**Location:** `src/streaming/sales_event_producer.py`

**Usage:**

```bash
python src/streaming/sales_event_producer.py [OPTIONS]
```

**Options:**

`--events COUNT` (optional)
- Number of events to generate
- Default: Infinite (continuous mode)
- Example: `--events 100`

`--interval SECONDS` (optional)
- Seconds between events
- Format: Float
- Default: 1.0
- Example: `--interval 0.5` (2 events/sec)

`--topic TOPIC_NAME` (optional)
- Kafka topic name
- Default: From KAFKA_TOPIC env variable
- Example: `--topic test_sales_events`

`--bootstrap-servers SERVERS` (optional)
- Kafka broker addresses
- Default: From KAFKA_BOOTSTRAP_SERVERS env variable
- Example: `--bootstrap-servers localhost:9092`

**Examples:**

```bash
# Generate 100 events at 1 event/sec
python src/streaming/sales_event_producer.py --events 100 --interval 1

# Continuous generation at 2 events/sec
python src/streaming/sales_event_producer.py --interval 0.5

# Fast test (10 events, no delay)
python src/streaming/sales_event_producer.py --events 10 --interval 0

# Custom topic
python src/streaming/sales_event_producer.py --topic test_events --events 50
```

**Output:**

```
[2024-01-15 10:30:01] INFO - Connected to Kafka broker: localhost:9092
[2024-01-15 10:30:01] INFO - Producing to topic: sales_events
[2024-01-15 10:30:01] INFO - Event 1/100 produced - Order ID: ORD-123456
[2024-01-15 10:30:02] INFO - Event 2/100 produced - Order ID: ORD-123457
```

## Validation Tools

### config_validator.py

Validate environment configuration and connectivity.

**Location:** `tools/validation/config_validator.py`

**Usage:**

```bash
python tools/validation/config_validator.py [OPTIONS]
```

**Options:**

`--validate-all` (optional)
- Run all validation checks
- Recommended for initial setup

`--validate-aws` (optional)
- AWS credentials and S3 access only

`--validate-snowflake` (optional)
- Snowflake connection only

`--validate-env` (optional)
- Environment variable presence only

**Examples:**

```bash
# Full validation
python tools/validation/config_validator.py --validate-all

# AWS only
python tools/validation/config_validator.py --validate-aws

# Snowflake only
python tools/validation/config_validator.py --validate-snowflake
```

**Output:**

```
Validating environment configuration...
[PASS] AWS credentials found
[PASS] S3 bucket names configured
[PASS] Snowflake credentials found
[PASS] All required variables present

Validating AWS connectivity...
[PASS] AWS credentials valid
[PASS] S3 access confirmed

Validating Snowflake connectivity...
[PASS] Snowflake connection successful
[PASS] Database SALES_DB accessible

All validations passed.
```

### setup_doctor.py

Comprehensive system health check.

**Location:** `tools/validation/setup_doctor.py`

**Usage:**

```bash
python tools/validation/setup_doctor.py
```

**No Options:** Runs all checks automatically

**Checks Performed:**

1. Environment configuration
2. Docker services
3. Airflow status
4. AWS connectivity
5. Snowflake connectivity
6. S3 buckets
7. Data layer health (Bronze/Silver/Gold)
8. Kafka broker

**Output:**

```
Running system health checks...

Environment Configuration: PASS
Docker Services: PASS (6/6 running)
Airflow Status: PASS (4 DAGs found)
AWS S3 Access: PASS
Snowflake Connection: PASS
Kafka Broker: PASS
Bronze Layer: PASS (files found)
Silver Layer: PASS (Delta Lake valid)
Gold Layer: PASS (tables found)

All systems operational.
```

## dbt Commands

### dbt CLI

Standard dbt commands for transformation management.

**Location:** `dbt/` directory

**Setup:**

```bash
cd dbt/
```

**Commands:**

**Install Dependencies:**

```bash
dbt deps
```

Installs packages defined in `packages.yml`.

**Run Models:**

```bash
# All models
dbt run --target dev

# Specific model
dbt run --select fact_sales --target dev

# Models in subdirectory
dbt run --select staging.* --target dev

# Production target
dbt run --target prod
```

**Test Data Quality:**

```bash
# All tests
dbt test --target dev

# Specific model tests
dbt test --select fact_sales --target dev

# Source freshness
dbt source freshness --target dev
```

**Generate Documentation:**

```bash
# Generate docs
dbt docs generate --target dev

# Serve docs (opens browser)
dbt docs serve --port 8081
```

**Debug Connection:**

```bash
dbt debug --target dev
```

**Compile SQL:**

```bash
# Compile without running
dbt compile --target dev

# View compiled SQL
cat target/compiled/sales_pipeline/models/marts/fact_sales.sql
```

**Snapshot SCD:**

```bash
# Run Type 2 snapshots
dbt snapshot --target dev
```

**Seed Data:**

```bash
# Load CSV seeds
dbt seed --target dev
```

## Airflow CLI

### airflow dags

Manage DAGs from command line.

**Execute via Docker:**

```bash
docker-compose exec airflow-webserver airflow dags COMMAND
```

**Commands:**

**List DAGs:**

```bash
docker-compose exec airflow-webserver airflow dags list
```

**Trigger DAG:**

```bash
docker-compose exec airflow-webserver airflow dags trigger batch_processing_dag
```

**Trigger with Config:**

```bash
docker-compose exec airflow-webserver airflow dags trigger batch_processing_dag \
  --conf '{"partition_date": "2024-01-15"}'
```

**Pause/Unpause:**

```bash
# Pause
docker-compose exec airflow-webserver airflow dags pause batch_processing_dag

# Unpause
docker-compose exec airflow-webserver airflow dags unpause batch_processing_dag
```

**Test DAG:**

```bash
docker-compose exec airflow-webserver airflow dags test batch_processing_dag 2024-01-15
```

### airflow tasks

Manage individual tasks.

**List Tasks:**

```bash
docker-compose exec airflow-webserver airflow tasks list batch_processing_dag
```

**Test Task:**

```bash
docker-compose exec airflow-webserver airflow tasks test \
  batch_processing_dag \
  check_new_files \
  2024-01-15
```

**Clear Task:**

```bash
docker-compose exec airflow-webserver airflow tasks clear \
  batch_processing_dag \
  -s 2024-01-15 \
  -e 2024-01-15 \
  -t process_batch_data
```

### airflow variables

Manage Airflow variables.

**Set Variable:**

```bash
docker-compose exec airflow-webserver airflow variables set \
  raw_bucket your-raw-bucket
```

**Get Variable:**

```bash
docker-compose exec airflow-webserver airflow variables get raw_bucket
```

**List Variables:**

```bash
docker-compose exec airflow-webserver airflow variables list
```

**Import from JSON:**

```bash
docker-compose exec airflow-webserver airflow variables import /path/to/variables.json
```

### airflow db

Database management.

**Initialize Database:**

```bash
docker-compose exec airflow-webserver airflow db init
```

**Upgrade Database:**

```bash
docker-compose exec airflow-webserver airflow db upgrade
```

**Clean Old Records:**

```bash
docker-compose exec airflow-webserver airflow db clean \
  --clean-before-timestamp "2024-01-01" \
  --yes
```

## Terraform Commands

### terraform init

Initialize Terraform working directory.

```bash
cd infrastructure/terraform
terraform init
```

**Options:**

`-upgrade`
- Upgrade providers to latest versions

### terraform plan

Preview infrastructure changes.

```bash
terraform plan -out=tfplan
```

**Options:**

`-out=FILE`
- Save plan to file for apply

`-var-file=FILE`
- Load variables from file

### terraform apply

Deploy infrastructure.

```bash
# Apply saved plan
terraform apply tfplan

# Apply with auto-approve
terraform apply -auto-approve
```

### terraform destroy

Remove all infrastructure.

```bash
terraform destroy
```

**Warning:** Irreversible operation

### terraform output

View output values.

```bash
# All outputs
terraform output

# Specific output
terraform output raw_bucket_name
```

### terraform state

Manage state.

```bash
# List resources
terraform state list

# Show resource
terraform state show aws_s3_bucket.raw

# Remove resource from state (advanced)
terraform state rm aws_s3_bucket.raw
```

## Docker Compose

### Service Management

**Start Services:**

```bash
# Foreground
docker-compose up

# Background
docker-compose up -d

# Rebuild images
docker-compose up --build
```

**Stop Services:**

```bash
# Stop services
docker-compose stop

# Stop and remove
docker-compose down

# Remove volumes (WARNING: deletes data)
docker-compose down -v
```

**Restart Services:**

```bash
# All services
docker-compose restart

# Specific service
docker-compose restart airflow-scheduler
```

**View Status:**

```bash
docker-compose ps
```

**View Logs:**

```bash
# All services
docker-compose logs

# Specific service
docker-compose logs airflow-scheduler

# Follow logs
docker-compose logs -f airflow-webserver

# Last N lines
docker-compose logs --tail=100 kafka
```

**Execute Commands:**

```bash
# Interactive shell
docker-compose exec airflow-webserver bash

# Single command
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## AWS CLI

### S3 Operations

**List Buckets:**

```bash
aws s3 ls
```

**List Objects:**

```bash
aws s3 ls s3://your-raw-bucket/sales_data/ --recursive
```

**Copy Files:**

```bash
# Download
aws s3 cp s3://your-raw-bucket/sales_data/file.csv .

# Upload
aws s3 cp file.csv s3://your-raw-bucket/sales_data/
```

**Sync Directories:**

```bash
# Upload directory
aws s3 sync data/csv/ s3://your-raw-bucket/sales_data/

# Download directory
aws s3 sync s3://your-raw-bucket/sales_data/ data/downloads/
```

**Remove Files:**

```bash
# Single file
aws s3 rm s3://your-raw-bucket/sales_data/file.csv

# Recursive
aws s3 rm s3://your-raw-bucket/sales_data/ --recursive
```

### IAM Operations

**Get Caller Identity:**

```bash
aws sts get-caller-identity
```

**List Roles:**

```bash
aws iam list-roles
```

**Get Role:**

```bash
aws iam get-role --role-name snowflake-s3-access-role
```

## SnowSQL

### Connection

**Connect:**

```bash
snowsql -a YOUR_ACCOUNT_NAME.REGION -u YOUR_USERNAME
```

**Execute Query:**

```bash
snowsql -a YOUR_ACCOUNT_NAME.REGION -u YOUR_USERNAME -q "SELECT COUNT(*) FROM fact_sales;"
```

**Execute File:**

```bash
snowsql -a YOUR_ACCOUNT_NAME.REGION -u YOUR_USERNAME -f script.sql
```

### Common Queries

**Show Databases:**

```sql
SHOW DATABASES;
```

**Show Tables:**

```sql
USE DATABASE SALES_DB;
SHOW TABLES IN SCHEMA PUBLIC;
```

**Describe Table:**

```sql
DESC TABLE fact_sales;
```

**Query History:**

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
ORDER BY START_TIME DESC
LIMIT 10;
```

## Spark Submit

### Local Spark

**Run Batch ETL:**

```bash
spark-submit \
  --master local[*] \
  --conf spark.executor.memory=4g \
  --conf spark.driver.memory=2g \
  src/spark/jobs/batch_etl.py
```

**Run Streaming ETL:**

```bash
spark-submit \
  --master local[*] \
  src/spark/jobs/streaming_etl.py
```

### Databricks Submit

Handled automatically when `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are set.

Manual submission via REST API:

```bash
curl -X POST \
  -H "Authorization: Bearer YOUR_DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "run_name": "batch_etl",
    "new_cluster": {
      "spark_version": "12.2.x-scala2.12",
      "node_type_id": "i3.xlarge",
      "num_workers": 2
    },
    "spark_python_task": {
      "python_file": "dbfs:/scripts/batch_etl.py"
    }
  }' \
  https://YOUR_WORKSPACE.cloud.databricks.com/api/2.1/jobs/runs/submit
```

## Utility Scripts

### export_tf_vars.sh

Export environment variables for Terraform.

```bash
source export_tf_vars.sh
```

Reads `.env` and exports as `TF_VAR_*` variables.

### cleanup_tests_schema.py

Clean up test schemas in Snowflake.

**Location:** `tools/cleanup_tests_schema.py`

```bash
python tools/cleanup_tests_schema.py
```

Removes test schemas and objects created during testing.
