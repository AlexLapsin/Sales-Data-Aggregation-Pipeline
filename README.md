# Sales Data Aggregation Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

![Apache Airflow 2.10](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow)
![Apache Spark 3.5.7](https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C?logo=apachespark)
![Apache Kafka 3.5+](https://img.shields.io/badge/Apache%20Kafka-3.5+-231F20?logo=apachekafka)
![Delta Lake 3.2.0](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD4)
![dbt 1.7+](https://img.shields.io/badge/dbt-1.7+-FF694B?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-29B5E8?logo=snowflake)
![Terraform 1.5+](https://img.shields.io/badge/Terraform-1.5+-7B42BC?logo=terraform)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazonaws)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)

Enterprise-grade sales data aggregation pipeline demonstrating Medallion architecture (Bronze → Silver → Gold) with production-ready patterns including Delta Lake, Snowflake integration, and comprehensive data quality controls.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MEDALLION DATA ARCHITECTURE                          │
└─────────────────────────────────────────────────────────────────────────────┘

DATA SOURCES                INGESTION              PROCESSING
┌──────────────┐         ┌──────────────┐      ┌──────────────┐
│              │         │              │      │              │
│ • CSV Files  │────────▶│ data_uploader│─────▶│   Airflow    │
│ • Kafka      │         │ • Kafka      │      │   • Batch    │
│   Events     │         │   Producer   │      │   • Streaming│
│              │         │              │      │   • Analytics│
└──────────────┘         └──────────────┘      └──────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              BRONZE LAYER                                    │
│  • AWS S3 Storage                                                           │
│  • Raw CSV/JSON files with date partitioning (year=YYYY/month=MM/day=DD/)  │
│  • Immutable audit trail, 90-day retention                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                              ┌──────────────┐
                              │ Spark ETL    │
                              │ • Validation │
                              │ • Cleaning   │
                              │ • Deduplication│
                              └──────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SILVER LAYER                                    │
│  • Delta Lake 3.2.0 (Parquet + Transaction Log)                            │
│  • ACID transactions, schema enforcement                                    │
│  • Time travel (30 days), automatic file management                        │
│  • Validated, unified data foundation                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ (Dataset Trigger)
                              ┌──────────────┐
                              │ dbt Models   │
                              │ • Staging    │
                              │ • Intermediate│
                              │ • Marts (SCD2)│
                              └──────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               GOLD LAYER                                     │
│  • Snowflake Cloud Data Warehouse                                          │
│  • Star Schema: fact_sales, dim_customer, dim_product, dim_date, dim_store │
│  • SCD Type 2 for slowly changing dimensions                               │
│  • Business-ready analytics tables                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

### Medallion Architecture
- **Bronze Layer**: S3-based raw data lake with date partitioning and immutable audit trail
- **Silver Layer**: Delta Lake with ACID transactions, schema enforcement, and data quality validation
- **Gold Layer**: Snowflake star schema with dimensional modeling and SCD Type 2 support

### Technology Stack
- **Orchestration**: Apache Airflow 2.10 with dataset-aware DAG triggers
- **Processing**: Apache Spark 3.5.7 with Delta Lake 3.2.0 for batch and streaming ETL
- **Transformations**: dbt 1.7+ with comprehensive data quality testing
- **Streaming**: Apache Kafka 3.5+ with Kafka Connect S3 sink
- **Data Warehouse**: Snowflake with key-pair authentication and AUTO_REFRESH external tables
- **Infrastructure**: Terraform 1.5+ for AWS S3, IAM, and Snowflake provisioning

### Data Quality & Reliability
- Two-tier deduplication strategy (row_id + business key)
- UUIDv7-based unique identifiers with millisecond precision
- Comprehensive dbt tests (uniqueness, not null, referential integrity, business logic)
- Data quality scoring system for record completeness
- Master Data Management (MDM) patterns for customer/product golden records

### Security & Compliance
- Snowflake RSA key-pair authentication (2048-bit encrypted keys)
- AWS IAM least privilege policies
- Encryption at rest (S3 SSE, Snowflake automatic) and in transit (TLS 1.2+)
- Secrets management with .env files (development) and AWS Secrets Manager (production)
- Container security with non-root users

### Developer Experience
- Complete Docker Compose environment for local development
- Comprehensive validation tools (config_validator, setup_doctor)
- Organized modular requirements files (core, cloud, streaming, spark, dev, test)
- Professional documentation following Diátaxis framework (Tutorial, How-To, Reference, Explanation)

## Quick Start

### Prerequisites
- **Docker** & **Docker Compose**
- **Python 3.9+**
- **AWS CLI** (configured with credentials)
- **Snowflake Account** (with key-pair authentication set up)
- **Git**

### 1. Clone and Configure

```bash
# Clone repository
git clone https://github.com/your-org/sales-data-aggregation-pipeline.git
cd sales-data-aggregation-pipeline

# Copy environment template
cp .env.example .env

# Edit .env with your credentials:
# - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# - RAW_BUCKET and PROCESSED_BUCKET (S3 bucket names)
# - SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_USER
# - SNOWFLAKE_PRIVATE_KEY_PATH and SNOWFLAKE_KEY_PASSPHRASE
nano .env
```

### 2. Deploy Infrastructure

```bash
# Export Terraform variables from .env
source export_tf_vars.sh

# Deploy AWS S3 buckets, IAM roles, and Snowflake external volume
cd infrastructure/terraform
terraform init
terraform plan
terraform apply

# Verify deployment
terraform output
```

### 3. Start Local Services

```bash
# Build and start all services (Airflow, Kafka, PostgreSQL, Redis)
docker-compose up --build

# Alternative: run in background
docker-compose up -d

# Monitor services
docker-compose ps
docker-compose logs -f airflow-scheduler
```

Wait approximately 2-3 minutes for Airflow initialization to complete.

### 4. Validate Setup

```bash
# Run comprehensive environment validation
python tools/validation/setup_doctor.py

# Validate configuration
python tools/validation/config_validator.py --validate-all
```

### 5. Access Services

- **Airflow UI**: http://localhost:8080 (credentials: admin/admin)
- **Kafka**: localhost:9092
- **PostgreSQL** (Airflow metadata): localhost:5432

## Usage

### Batch Data Processing

#### Upload CSV Data to Bronze Layer

```bash
# Upload sample CSV file
python src/bronze/data_uploader.py --source data/sample_sales.csv

# Verify upload in S3
aws s3 ls s3://YOUR_RAW_BUCKET/sales_data/ --recursive
```

The data_uploader automatically:
- Validates CSV schema (24-field canonical format)
- Generates date-based partitions (year=YYYY/month=MM/day=DD/)
- Uploads to S3 Bronze layer

#### Trigger Batch Processing DAG

1. Open Airflow UI: http://localhost:8080
2. Locate `batch_processing_dag`
3. Enable the DAG (toggle switch)
4. Click "Trigger DAG" to start processing

The batch_processing_dag will:
- Detect new files in S3 Bronze layer
- Run Spark ETL job with validation, cleaning, and deduplication
- Write to Delta Lake Silver layer
- Automatically trigger `analytics_dag` via dataset event

#### Monitor Analytics DAG

The `analytics_dag` runs automatically after batch processing completes:
- Executes dbt staging models (stg_customers_from_sales, stg_products_from_sales, stg_sales_silver)
- Builds intermediate models with SCD2 logic (int_product_scd2, int_store_scd2, int_sales_unified)
- Creates Gold layer dimensions (dim_customer, dim_product, dim_store, dim_date)
- Builds fact table (fact_sales)
- Runs comprehensive dbt tests

### Streaming Data Processing

#### Start Kafka Producer

```bash
# Generate 1000 sales events at 1-second intervals
python src/streaming/sales_event_producer.py --events 1000 --interval 1

# Generate continuous stream
python src/streaming/sales_event_producer.py --continuous
```

#### Deploy Kafka Connect S3 Sink

```bash
# Deploy connector to write Kafka events to S3 Bronze layer
bash deploy/streaming/deploy_connector.sh

# Verify connector status
curl http://localhost:8083/connectors/s3-sink-sales/status | jq
```

#### Trigger Streaming Processing DAG

1. Open Airflow UI: http://localhost:8080
2. Locate `streaming_processing_dag`
3. Enable and trigger the DAG

The streaming_processing_dag will:
- Detect JSON files in S3 Bronze layer (sales_events/ prefix)
- Run Spark streaming job with schema normalization
- Merge into Delta Lake Silver layer (upsert operation)
- Trigger `analytics_dag` for incremental dbt processing

### dbt Transformations

```bash
# Navigate to dbt directory
cd dbt/

# Install dbt dependencies
dbt deps

# Run all transformations
dbt run --target dev

# Run data quality tests
dbt test --target dev

# Run specific models
dbt run --select staging --target dev
dbt run --select intermediate --target dev
dbt run --select marts --target dev

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

### Verify Data in Snowflake

```sql
-- Connect to Snowflake and verify Gold layer

-- Check dimension row counts
SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM SALES_DB.PUBLIC.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM SALES_DB.PUBLIC.dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM SALES_DB.PUBLIC.dim_store
UNION ALL
SELECT 'dim_date', COUNT(*) FROM SALES_DB.PUBLIC.dim_date;

-- Check fact table
SELECT COUNT(*) as total_sales FROM SALES_DB.PUBLIC.fact_sales;

-- Verify SCD2 integrity (should have no duplicates with is_current = TRUE)
SELECT customer_id, COUNT(*) as duplicate_count
FROM SALES_DB.PUBLIC.dim_customer
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check data quality scores
SELECT
    CASE
        WHEN data_quality_score >= 90 THEN '90-100 (Excellent)'
        WHEN data_quality_score >= 80 THEN '80-89 (Good)'
        WHEN data_quality_score >= 70 THEN '70-79 (Acceptable)'
        ELSE '<70 (Poor)'
    END as quality_tier,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM SALES_DB.PUBLIC.fact_sales
GROUP BY quality_tier
ORDER BY quality_tier DESC;
```

## Documentation

Complete documentation organized using the Diátaxis framework:

### Getting Started
- **[Tutorial: Getting Started](docs/tutorial/getting-started.md)** - Complete 60-minute walkthrough from installation to first pipeline run

### How-To Guides
- **[Deploy Infrastructure](docs/how-to/deploy-infrastructure.md)** - Terraform deployment for AWS and Snowflake
- **[Configure Environment](docs/how-to/configure-environment.md)** - Local development setup
- **[Run Batch Pipeline](docs/how-to/run-batch-pipeline.md)** - CSV ingestion and processing
- **[Run Streaming Pipeline](docs/how-to/run-streaming-pipeline.md)** - Kafka streaming setup
- **[Troubleshooting Guide](docs/how-to/troubleshooting.md)** - Common issues and solutions

### Reference Documentation
- **[Architecture](docs/reference/architecture.md)** - System design and data flow
- **[Data Dictionary](docs/reference/data-dictionary.md)** - Complete schema definitions
- **[Configuration Reference](docs/reference/configuration.md)** - Environment variables and settings
- **[CLI Reference](docs/reference/cli-reference.md)** - Command-line tools

### Explanation
- **[Medallion Architecture](docs/explanation/medallion-architecture.md)** - Bronze/Silver/Gold design principles
- **[Technology Choices](docs/explanation/technology-choices.md)** - Technology selection rationale
- **[Architecture Decision Records](docs/explanation/adr/)** - Historical design decisions

## Testing

### Run Complete Test Suite

```bash
# Full test suite with coverage report
python tools/testing/run_tests.py

# Unit tests only
python tools/testing/run_tests.py --unit-only

# Integration tests only
python tools/testing/run_tests.py --integration-only
```

### Run Specific Test Categories

```bash
# Bronze layer tests
pytest tests/bronze/ -v

# Silver layer (Spark ETL) tests
pytest tests/silver/ -v

# Integration tests
pytest tests/integration/ -v

# Streaming tests
python tests/streaming/test_kafka_producer.py --mode all
```

### Run dbt Data Quality Tests

```bash
cd dbt/

# All dbt tests
dbt test --target dev

# Specific test categories
dbt test --select source:*          # Source data tests
dbt test --select staging.*         # Staging model tests
dbt test --select marts.*           # Gold layer tests
dbt test --select test_type:singular # Custom business logic tests
```

## Configuration

### Required Environment Variables

Create a `.env` file with the following configuration:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID="your_access_key"
AWS_SECRET_ACCESS_KEY="your_secret_key"
AWS_DEFAULT_REGION="us-east-1"

# S3 Buckets
RAW_BUCKET="your-project-raw-bucket"
PROCESSED_BUCKET="your-project-processed-bucket"

# Snowflake Configuration (Key-Pair Authentication)
SNOWFLAKE_ACCOUNT_NAME="xy12345"
SNOWFLAKE_ORGANIZATION_NAME="your_org"
SNOWFLAKE_USER="YOUR_USER"
SNOWFLAKE_PRIVATE_KEY_PATH="F:/path/to/rsa_key.p8"
SNOWFLAKE_KEY_PASSPHRASE="your_passphrase"
SNOWFLAKE_ROLE="ACCOUNTADMIN"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_DATABASE="SALES_DB"
SNOWFLAKE_SCHEMA="PUBLIC"

# Snowflake AWS Integration (from Terraform output)
SNOWFLAKE_IAM_USER_ARN="arn:aws:iam::123456789012:user/abc12345-s"
SNOWFLAKE_EXTERNAL_ID="ABC12345_SFCRole=123_..."

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
KAFKA_TOPIC="sales_events"

# Project Configuration
PROJECT_NAME="sales-data-pipeline"
ENVIRONMENT="dev"
ALLOWED_CIDR="your.ip.address/32"
```

See **[Configuration Reference](docs/reference/configuration.md)** for complete documentation.

### Key-Pair Authentication Setup

Snowflake requires RSA key-pair authentication:

1. Generate RSA key pair:
```bash
mkdir -p config/keys
cd config/keys

# Generate private key with passphrase
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

2. Register public key in Snowflake:
```sql
ALTER USER YOUR_USER SET RSA_PUBLIC_KEY='<paste_your_public_key_here>';
```

3. Update `.env`:
```bash
SNOWFLAKE_PRIVATE_KEY_PATH="F:/GITHUB/sales_data_aggregation_pipeline/config/keys/rsa_key.p8"
SNOWFLAKE_KEY_PASSPHRASE="your_passphrase"
```

## Troubleshooting

### Common Issues

#### 1. Airflow Webserver Not Starting
```bash
# Check Airflow logs
docker-compose logs airflow-webserver

# Wait 2-3 minutes for initialization
# Airflow requires database migration on first startup
```

#### 2. S3 Access Denied
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Test bucket access
aws s3 ls s3://YOUR_RAW_BUCKET/

# Check IAM permissions in AWS Console
```

#### 3. Snowflake Connection Failed
```bash
# Test key-pair authentication
python -c "
import os
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

with open(os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'), 'rb') as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=os.getenv('SNOWFLAKE_KEY_PASSPHRASE').encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    account=f\"{os.getenv('SNOWFLAKE_ACCOUNT_NAME')}.{os.getenv('SNOWFLAKE_ORGANIZATION_NAME')}\",
    private_key=pkb,
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
)
print('Connection successful!')
conn.close()
"
```

#### 4. dbt Tests Failing
```bash
cd dbt/

# Run with verbose output
dbt test --target dev --debug

# Test specific model
dbt test --select dim_customer --target dev

# Check compiled SQL
cat target/compiled/sales_dw/models/marts/dim_customer.sql
```

#### 5. Delta Lake Parquet Read Errors
```bash
# Verify Delta log exists
aws s3 ls s3://YOUR_PROCESSED_BUCKET/silver/sales/_delta_log/

# Check Spark version compatibility (should be 3.5.7)
docker-compose exec etl-pipeline spark-submit --version

# Verify Delta Lake version (should be 3.2.0)
```

For comprehensive troubleshooting, see **[Troubleshooting Guide](docs/how-to/troubleshooting.md)**.

## Project Structure

```
sales_data_aggregation_pipeline/
├── airflow/dags/               # Airflow DAGs
│   ├── batch_processing_dag.py        # Batch ETL orchestration
│   ├── streaming_processing_dag.py    # Streaming ETL orchestration
│   ├── analytics_dag.py               # dbt transformations (dataset-triggered)
│   └── kafka_connector_monitor_dag.py # Kafka Connect health monitoring
├── dbt/                        # dbt transformations
│   ├── models/
│   │   ├── staging/            # Source data extraction and cleaning
│   │   ├── intermediate/       # SCD2 and unified models
│   │   └── marts/              # Star schema dimensions and facts
│   ├── tests/singular/         # Custom business logic tests
│   └── macros/                 # Reusable dbt macros
├── src/
│   ├── bronze/                 # Data ingestion
│   │   └── data_uploader.py    # CSV to S3 upload utility
│   ├── spark/jobs/             # Spark ETL jobs
│   │   └── batch_etl.py        # Batch processing with validation
│   ├── streaming/              # Kafka components
│   │   └── sales_event_producer.py # Event generator
│   └── validation/             # Data validation utilities
├── infrastructure/
│   ├── terraform/              # Infrastructure as Code
│   │   ├── main.tf             # Root Terraform configuration
│   │   ├── modules/
│   │   │   ├── iam/            # AWS IAM roles and policies
│   │   │   └── snowflake/      # Snowflake resources
│   │   └── variables.tf        # Terraform input variables
│   └── docker/                 # Docker images
│       ├── Dockerfile.spark    # Spark with Delta Lake
│       ├── Dockerfile.dbt      # dbt image
│       └── airflow/Dockerfile-airflow # Airflow with dbt venv
├── deploy/
│   ├── snowflake/              # Snowflake deployment scripts
│   └── streaming/              # Kafka Connect deployment
│       ├── deploy_connector.sh # S3 sink connector setup
│       └── run_producer.sh     # Kafka producer startup
├── tools/
│   ├── validation/             # Configuration validation
│   │   ├── config_validator.py # Environment validation
│   │   └── setup_doctor.py     # Comprehensive health check
│   └── testing/                # Test utilities
│       └── run_tests.py        # Test runner
├── tests/                      # Test suite
│   ├── bronze/                 # Bronze layer tests
│   ├── silver/                 # Silver layer tests
│   ├── integration/            # Integration tests
│   └── streaming/              # Kafka streaming tests
├── docs/                       # Documentation (Diátaxis framework)
│   ├── tutorial/               # Learning-oriented guides
│   ├── how-to/                 # Task-oriented guides
│   ├── reference/              # Information-oriented docs
│   └── explanation/            # Understanding-oriented docs
├── requirements/               # Python dependencies
│   ├── core.txt                # Core pipeline dependencies
│   ├── cloud.txt               # Snowflake, AWS SDK
│   ├── streaming.txt           # Kafka dependencies
│   ├── spark.txt               # PySpark, Delta Lake
│   ├── dev.txt                 # Development tools
│   └── test.txt                # Testing frameworks
├── .env.example                # Environment template
├── docker-compose.yml          # Local development services
├── export_tf_vars.sh           # Terraform variable export script
└── README.md                   # This file
```

## Data Schema

### Canonical 24-Field Schema

All layers (Bronze, Silver, Gold) maintain a consistent 24-field schema:

```
Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Customer Name,
Segment, City, State, Country, Postal Code, Market, Region, Product ID, Category,
Sub-Category, Product Name, Sales, Quantity, Discount, Profit, Shipping Cost, Order Priority
```

### Star Schema (Gold Layer)

**Fact Table:**
- `fact_sales`: Sales transactions with foreign keys to dimensions

**Dimension Tables:**
- `dim_customer`: Customer dimension with SCD Type 2
- `dim_product`: Product dimension with SCD Type 2
- `dim_store`: Store/location dimension with SCD Type 2
- `dim_date`: Date dimension for time-based analysis

See **[Data Dictionary](docs/reference/data-dictionary.md)** for complete schema documentation.

## Development

### Code Standards

- **Python**: Black formatting, Flake8 linting, MyPy type hints
- **SQL/dbt**: Standard dbt conventions with documented models
- **Testing**: Minimum 90% coverage target
- **Documentation**: Update docs/ for all user-facing changes
- **Professional Tone**: No emojis or marketing language

### Development Workflow

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes with tests
3. Run test suite: `python tools/testing/run_tests.py`
4. Format and lint: `black . && flake8 . && mypy src/`
5. Commit: `git commit -am 'Add feature'`
6. Push: `git push origin feature/your-feature`
7. Create Pull Request

### Adding New Features

1. Update source code (src/streaming/, src/spark/, dbt/models/)
2. Add comprehensive tests (tests/)
3. Update Airflow DAGs if needed (airflow/dags/)
4. Run full test suite
5. Validate with setup_doctor: `python tools/validation/setup_doctor.py`
6. Update documentation (docs/)

## Known Limitations

### Current Constraints

1. **Cloud Dependencies**: Pipeline requires AWS S3 and Snowflake (no local-only mode)
2. **Snowflake Trial**: 30-day trial with $400 credits (production requires paid account)
3. **Kafka Connect**: Single-node development setup (production needs multi-broker cluster)
4. **Delta Lake Time Travel**: 30-day retention (configurable but consumes storage)

### Future Improvements

1. **Monitoring**: Prometheus/Grafana dashboards, data quality monitoring (Monte Carlo, Great Expectations)
2. **Performance**: Snowflake clustering keys, Delta Lake Z-ordering, Airflow parallelism tuning
3. **Data Quality**: Expanded dbt test coverage, custom macros, automated quarantine handling
4. **CI/CD**: GitHub Actions for testing, automated deployment pipelines, blue/green deployments
5. **Cost Optimization**: S3 lifecycle policies, Snowflake auto-suspend tuning, spot instances

## Recent Improvements

### Phase 7 - Data Quality Fixes (2025-11-02)

**Critical Bug Fixes:**
- Fixed 95.9% data loss in fact_sales caused by incorrect SCD2 effective_date logic in int_product_scd2 and int_store_scd2
- Eliminated 22 duplicate customer_ids by implementing MDM deduplication in stg_customers_from_sales
- Resolved 8 NULL customer names by prioritizing non-NULL values in customer deduplication
- Implemented two-tier deduplication in Spark ETL (row_id + business key)
- Fixed category validation to accept all 8 legitimate categories
- Adopted UUIDv7 for row_id generation (millisecond-precision sortable UUIDs)

**Results:**
- All dbt tests passing
- Dimension joins working correctly (NULL foreign keys reduced from 77%/90% to <1%)
- No duplicate current records in SCD2 dimensions
- Comprehensive data quality scoring implemented

### Phase 6 - Documentation Overhaul (2025-01-23)

- Implemented Diátaxis framework for documentation organization
- Created 13 professional documentation files across tutorial/how-to/reference/explanation
- Added Architecture Decision Records (ADRs)
- Archived legacy verbose documentation

### Phase 5 - Security Audit (2025-01-23)

- Implemented Snowflake key-pair authentication (RSA 2048-bit)
- Container security hardening (non-root users)
- IaC security scanning with tfsec
- Dependency vulnerability scanning with safety and pip-audit

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Apache Software Foundation for Airflow, Kafka, and Spark
- dbt Labs for the dbt transformation framework
- Snowflake for cloud data warehouse platform
- Delta Lake project for ACID storage layer
- AWS for cloud infrastructure services

---

**For questions, issues, or contributions, please open an issue on GitHub.**
