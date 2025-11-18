# Sales Data Aggregation Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-Ready-brightgreen?logo=githubactions)](https://github.com/features/actions)

![Apache Airflow 2.10](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow)
![Apache Spark 3.5.7](https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C?logo=apachespark)
![Apache Kafka 3.5+](https://img.shields.io/badge/Apache%20Kafka-3.5+-231F20?logo=apachekafka)
![Delta Lake 3.2.0](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD4)
![dbt 1.7+](https://img.shields.io/badge/dbt-1.7+-FF694B?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-29B5E8?logo=snowflake)
![Terraform 1.5+](https://img.shields.io/badge/Terraform-1.5+-7B42BC?logo=terraform)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazonaws)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker)

## Table of Contents

<table>
<tr>
<td>

- [Overview](#overview)
- [Architecture Overview](#architecture-overview)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Usage](#usage)

</td>
<td>

- [Documentation](#documentation)
- [Testing](#testing)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)

</td>
<td>

- [Data Schema](#data-schema)
- [Known Limitations](#known-limitations)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

</td>
</tr>
</table>

## Overview

A sales data aggregation pipeline that implements the Medallion architecture (Bronze → Silver → Gold) to process both batch and streaming data into a Snowflake analytics platform.

The pipeline ingests data from CSV files and Kafka streams, validates and transforms it using Apache Spark, stores the results in Delta Lake with ACID guarantees, applies SCD Type 2 dimensional modeling with dbt, and delivers analytics-ready tables to Snowflake. Apache Airflow orchestrates the entire workflow with dataset-aware triggers.

**Technologies:** Apache Airflow 2.10, Apache Spark 3.5.7, Delta Lake 3.2.0, Apache Kafka 3.5+, dbt 1.7+, Snowflake, Terraform 1.5+, AWS S3, Docker

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

**Production-Ready Architecture**
- Medallion data lakehouse (Bronze → Silver → Gold) with ACID guarantees
- SCD Type 2 dimensional modeling for historical change tracking
- Automated orchestration with dataset-aware dependency triggers

**Dual-Mode Data Processing**
- Batch ingestion from CSV files with validation and partitioning
- Real-time streaming from Apache Kafka with exactly-once semantics
- Unified Delta Lake storage layer for both processing modes

**Enterprise Data Quality**
- Automated deduplication and validation at multiple pipeline stages
- Comprehensive dbt testing (schema, referential integrity, business rules)
- Master Data Management patterns for customer and product golden records
- End-to-end encryption and key-pair authentication

## Quick Start

Get the pipeline running in 60 minutes with the **[Complete Tutorial](docs/tutorial/getting-started.md)**.

**Prerequisites:**
- Docker Desktop & Docker Compose
- Python 3.9+
- AWS CLI (configured with credentials)
- Snowflake account (with RSA key-pair authentication)

**Three Steps to Running Pipeline:**

```bash
# 1. Clone & Configure
git clone https://github.com/your-org/sales-data-aggregation-pipeline.git
cd sales-data-aggregation-pipeline
cp .env.example .env
# Edit .env with your AWS and Snowflake credentials

# 2. Deploy Infrastructure (Terraform)
source export_tf_vars.sh
cd infrastructure/terraform && terraform init && terraform apply
cd ../..

# 3. Start Services
docker-compose up -d
```

**Next Steps:**
- Access **Airflow UI**: http://localhost:8080 (credentials: admin/admin)
- Enable and trigger `batch_processing_dag` (handles data upload and processing through Bronze → Silver → Gold)
- **[Complete Tutorial](docs/tutorial/getting-started.md)** - 60-minute guided walkthrough
- **[Configure Environment](docs/how-to/configure-environment.md)** - Detailed local setup

## Usage

### Batch Processing (CSV Files)

```bash
# Trigger Airflow DAG via UI (handles data upload automatically)
# 1. Open http://localhost:8080
# 2. Enable and trigger 'batch_processing_dag'
# 3. Monitor 'analytics_dag' (auto-triggered)

# Optional: Manual upload to Bronze layer (if needed outside of DAG)
python src/bronze/data_uploader.py --source data/sample_sales.csv
```

**Pipeline Flow:** CSV → S3 Bronze → Spark ETL → Delta Lake Silver → dbt Transformations → Snowflake Gold

### Streaming Processing (Kafka)

```bash
# Generate sales events
python src/streaming/sales_event_producer.py --events 1000 --interval 1

# Deploy Kafka Connect S3 sink
bash deploy/streaming/deploy_connector.sh

# Trigger 'streaming_processing_dag' in Airflow UI
```

**Pipeline Flow:** Kafka Events → S3 Bronze → Spark Streaming → Delta Lake Silver (upsert) → dbt Incremental → Snowflake Gold

### dbt Transformations

```bash
cd dbt/

# Run transformations and tests
dbt run --target dev
dbt test --target dev

# Generate documentation
dbt docs generate && dbt docs serve
```

**Complete Guides:**
- **[Run Batch Pipeline](docs/how-to/run-batch-pipeline.md)** - Detailed batch processing
- **[Run Streaming Pipeline](docs/how-to/run-streaming-pipeline.md)** - Kafka streaming setup
- **[Verify Data in Snowflake](docs/how-to/run-batch-pipeline.md#verify-data-in-snowflake)** - SQL validation queries

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

All configuration is managed through environment variables in the `.env` file.

**Setup:**
```bash
cp .env.example .env
# Edit .env with your credentials
```

**Essential Variables:**
```bash
# AWS
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
RAW_BUCKET=your-project-raw-bucket
PROCESSED_BUCKET=your-project-processed-bucket

# Snowflake (RSA Key-Pair Authentication)
SNOWFLAKE_ACCOUNT_NAME=xy12345
SNOWFLAKE_ORGANIZATION_NAME=your_org
SNOWFLAKE_USER=YOUR_USER
SNOWFLAKE_PRIVATE_KEY_PATH=/config/keys/rsa_key.p8
SNOWFLAKE_KEY_PASSPHRASE=your_passphrase
```

**Complete Reference:** [Configuration Guide](docs/reference/configuration.md)

**Key-Pair Setup:** [Deploy Infrastructure](docs/how-to/deploy-infrastructure.md#snowflake-key-pair-setup)

## Troubleshooting

For common issues and solutions, see the [Troubleshooting Guide](docs/how-to/troubleshooting.md).

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

All layers (Bronze, Silver, Gold) maintain this consistent schema:

```
Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Customer Name,
Segment, City, State, Country, Postal Code, Market, Region, Product ID, Category,
Sub-Category, Product Name, Sales, Quantity, Discount, Profit, Shipping Cost, Order Priority
```

### Star Schema (Gold Layer)

**Fact Table:** `fact_sales` (sales transactions with foreign keys to dimensions)

**Dimension Tables:**
- `dim_customer` - Customer dimension with SCD Type 2
- `dim_product` - Product dimension with SCD Type 2
- `dim_store` - Store/location dimension with SCD Type 2
- `dim_date` - Date dimension for time-based analysis

**Complete Schema:** [Data Dictionary](docs/reference/data-dictionary.md)

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

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:
- Reporting issues
- Submitting pull requests
- Development setup
- Code standards and testing

For questions, open a GitHub issue or discussion.

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

[⬆ Back to Top](#sales-data-aggregation-pipeline)
