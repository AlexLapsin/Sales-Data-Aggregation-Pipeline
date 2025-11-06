# Configure Environment

Set up local development environment with Docker and service configuration.

## Prerequisites

- Docker Desktop 20.10+ installed and running
- Docker Compose 2.0+ installed
- Git repository cloned
- Infrastructure deployed (see [Deploy Infrastructure](deploy-infrastructure.md))

## Create Environment File

Copy the environment template:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1

# S3 Buckets (from Terraform outputs)
RAW_BUCKET=your-project-raw-bucket
PROCESSED_BUCKET=your-project-processed-bucket

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=SALES_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Databricks (Optional - for production Spark workloads)
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=sales_events

# Project Settings
PROJECT_NAME=sales-pipeline
ENVIRONMENT=dev
```

## Validate Configuration

Run the configuration validator:

```bash
python tools/validation/config_validator.py --validate-all
```

Expected output:
```
Validating AWS credentials... PASS
Validating Snowflake connection... PASS
Validating S3 buckets... PASS
Validating environment variables... PASS

All validations passed.
```

If any validations fail, check the error messages and update `.env` accordingly.

## Verify Docker Installation

Check Docker is running:

```bash
docker --version
docker-compose --version
```

Expected output:
```
Docker version 20.10.x or higher
Docker Compose version 2.x.x or higher
```

Verify Docker daemon is running:

```bash
docker ps
```

Expected output: List of running containers (may be empty)

## Build Docker Images

Build all service images:

```bash
docker-compose build
```

This builds custom images for:
- `airflow-webserver` - Airflow UI and scheduler
- `spark-processor` - PySpark job executor
- `dbt-transformer` - dbt transformations

Build time: 5-10 minutes (first time), 1-2 minutes (subsequent builds)

Expected output:
```
Building airflow-webserver
Step 1/12 : FROM apache/airflow:2.7.3-python3.9
...
Successfully built abc123def456
Successfully tagged sales_pipeline_airflow-webserver:latest
```

## Start Services

Start all services in detached mode:

```bash
docker-compose up -d
```

Services started:
- `postgres` - Airflow metadata database
- `redis` - Airflow task queue
- `airflow-webserver` - Airflow UI (port 8080)
- `airflow-scheduler` - DAG scheduler
- `airflow-triggerer` - Event-driven tasks
- `zookeeper` - Kafka coordination
- `kafka` - Message broker (port 9092)

Wait 2-3 minutes for initialization. Services are ready when:

```bash
docker-compose ps
```

Expected output: All services showing `Up` state

```
NAME                    STATUS              PORTS
postgres                Up (healthy)        5432/tcp
redis                   Up (healthy)        6379/tcp
airflow-webserver       Up (healthy)        0.0.0.0:8080->8080/tcp
airflow-scheduler       Up (healthy)
airflow-triggerer       Up (healthy)
zookeeper               Up                  2181/tcp
kafka                   Up                  0.0.0.0:9092->9092/tcp
```

## Initialize Airflow

Airflow auto-initializes on first startup. Verify initialization:

```bash
docker-compose logs airflow-webserver | grep "Listening at"
```

Expected output:
```
airflow-webserver | [timestamp] INFO - Listening at: http://0.0.0.0:8080
```

Access Airflow UI at http://localhost:8080

Login credentials:
- Username: `admin`
- Password: `admin`

## Configure Airflow Variables

Set required Airflow variables via UI or CLI:

**Via UI:**
1. Navigate to Admin > Variables
2. Add these variables:

| Key | Value |
|-----|-------|
| `aws_access_key_id` | Your AWS access key |
| `aws_secret_access_key` | Your AWS secret key |
| `raw_bucket` | Your raw bucket name |
| `processed_bucket` | Your processed bucket name |
| `snowflake_account` | Your Snowflake account |
| `snowflake_user` | Your Snowflake username |
| `snowflake_password` | Your Snowflake password |

**Via CLI:**

```bash
docker-compose exec airflow-webserver airflow variables set aws_access_key_id "your_key"
docker-compose exec airflow-webserver airflow variables set aws_secret_access_key "your_secret"
docker-compose exec airflow-webserver airflow variables set raw_bucket "your-project-raw-bucket"
docker-compose exec airflow-webserver airflow variables set processed_bucket "your-project-processed-bucket"
```

**Note:** The `docker-compose.yml` includes initialization scripts that automatically set these variables from `.env` on startup.

## Verify Service Connectivity

Test AWS connection:

```bash
docker-compose exec airflow-webserver aws s3 ls
```

Expected output: List of your S3 buckets

Test Snowflake connection:

```bash
docker-compose exec airflow-webserver python -c "
from snowflake.connector import connect
import os
conn = connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD')
)
print('Snowflake connection successful')
conn.close()
"
```

Expected output: `Snowflake connection successful`

Test Kafka connection:

```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output: List of Kafka topics (may be empty initially)

## Service Logs

View logs for all services:

```bash
docker-compose logs -f
```

View logs for specific service:

```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f kafka
```

Check for errors:

```bash
docker-compose logs | grep ERROR
```

## Stop Services

Stop all services:

```bash
docker-compose down
```

Stop and remove volumes (caution: removes all data):

```bash
docker-compose down -v
```

## Health Checks

Run comprehensive health check:

```bash
python tools/validation/setup_doctor.py
```

Expected output:
```
Checking Docker services... PASS
Checking Airflow... PASS
Checking Kafka... PASS
Checking AWS connectivity... PASS
Checking Snowflake connectivity... PASS
Checking S3 buckets... PASS

All health checks passed.
```

## Environment-Specific Configuration

**Development Environment:**
Use `docker-compose.yml` (default):
```bash
docker-compose up -d
```

**Production Environment:**
Use `docker-compose-cloud.yml`:
```bash
docker-compose -f docker-compose-cloud.yml up -d
```

Differences:
- Production uses external managed services (AWS RDS, ElastiCache)
- Development uses local containers (PostgreSQL, Redis)
- Production has monitoring and logging integrations

## Troubleshooting

**Error: "Port 8080 already in use"**
- Check if another service is using port 8080: `lsof -i :8080`
- Change Airflow port in `docker-compose.yml`: `8081:8080`

**Error: "Cannot connect to Docker daemon"**
- Verify Docker Desktop is running
- Check Docker daemon status: `docker info`

**Error: "OCI runtime create failed"**
- Increase Docker memory allocation in Docker Desktop settings
- Minimum: 4GB RAM, Recommended: 8GB RAM

**Services not becoming healthy:**
- Check service logs: `docker-compose logs [service-name]`
- Increase startup wait time (services may take 2-3 minutes)
- Verify `.env` has all required variables

**Airflow DAGs not appearing:**
- Wait 2-3 minutes for DAG parsing
- Check scheduler logs: `docker-compose logs airflow-scheduler`
- Verify `airflow/dags/` directory is mounted: `docker-compose exec airflow-webserver ls /opt/airflow/dags`

## Next Steps

- [Run Batch Pipeline](run-batch-pipeline.md) - Execute CSV ingestion
- [Run Streaming Pipeline](run-streaming-pipeline.md) - Execute Kafka streaming
