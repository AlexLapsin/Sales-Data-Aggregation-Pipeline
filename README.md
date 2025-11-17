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

## Table of Contents

<table>
<tr>
<td>

- [Overview](#overview)
- [Demo](#demo)
- [Highlights](#highlights)
- [Who This Is For](#who-this-is-for)
- [Real-World Use Case](#real-world-use-case)
- [Architecture Overview](#architecture-overview)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Documentation](#documentation)
- [Testing](#testing)

</td>
<td>

- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Data Schema](#data-schema)
- [Development](#development)
- [Known Limitations](#known-limitations)
- [Recent Improvements](#recent-improvements)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

</td>
</tr>
</table>

## Overview

This project is an **end-to-end data aggregation pipeline** for retail sales data, designed to demonstrate production-ready data engineering patterns. It implements a complete **Medallion architecture** (Bronze → Silver → Gold) with enterprise-grade features including ACID transactions, slowly changing dimensions, comprehensive data quality controls, and cloud-native deployment.

**Key Technologies:** Apache Airflow 2.10, Apache Spark 3.5.7, Delta Lake 3.2.0, Apache Kafka 3.5+, dbt 1.7+, Snowflake, Terraform 1.5+, AWS S3, Docker

**Implementation:** Fully functional batch and streaming pipelines with infrastructure-as-code deployment, automated data quality testing, and professional documentation.

## Demo

**Coming Soon:** Video walkthrough demonstrating the complete pipeline from data ingestion to Snowflake analytics.

**What the demo will cover:**
- Local environment setup with Docker Compose
- CSV batch ingestion through Bronze → Silver → Gold layers
- Real-time Kafka streaming with S3 sink connector
- dbt transformations building dimensional model
- Snowflake query examples showing SCD Type 2 in action
- Data quality validation and testing

**In the meantime:** Follow the **[Getting Started Tutorial](docs/tutorial/getting-started.md)** for a complete 60-minute guided walkthrough.

## Highlights

**End-to-End Production Pipeline**
- Orchestrates both batch (CSV) and streaming (Kafka) data ingestion
- ACID-compliant Delta Lake Silver layer with schema enforcement and time travel
- Snowflake Gold layer with SCD Type 2 slowly changing dimensions
- Dataset-aware Airflow DAG triggers for automated workflow coordination

**Production-Ready Architecture**
- Medallion architecture (Bronze/Silver/Gold) with clear separation of concerns
- Two-tier deduplication strategy (UUIDv7 row IDs + business key validation)
- Master Data Management (MDM) patterns for customer and product golden records
- Comprehensive dbt testing (90+ tests) covering uniqueness, referential integrity, and business logic

**Enterprise Security**
- RSA key-pair authentication for Snowflake (2048-bit encrypted keys)
- AWS IAM least privilege policies with resource-level permissions
- Encryption at rest (S3 SSE, Snowflake) and in transit (TLS 1.2+)
- Container security with non-root users and minimal attack surface

**Developer Experience**
- Complete Docker Compose environment for local development (no cloud required for testing orchestration)
- Infrastructure-as-Code with Terraform for reproducible deployments
- Automated validation tools (config_validator, setup_doctor) for environment health checks
- Professional documentation following Diátaxis framework (Tutorial, How-To, Reference, Explanation)

## Who This Is For

**Data Engineers**
Reference implementation of modern lakehouse architecture with Delta Lake, Airflow orchestration, and dbt transformations. Demonstrates production patterns for data quality, SCD Type 2, and ACID transactions.

**Analytics Engineers**
Comprehensive dbt project with staging, intermediate, and marts layers. Includes 90+ tests, custom macros, and dimensional modeling with slowly changing dimensions.

**Platform Engineers / DevOps**
Infrastructure-as-Code deployment with Terraform, Docker Compose orchestration, and security best practices. Demonstrates cloud-native architecture with AWS S3 and Snowflake integration.

**Hiring Managers / Recruiters**
Portfolio project showcasing end-to-end data pipeline development, modern tooling proficiency, and software engineering best practices (testing, documentation, version control).

## Real-World Use Case

**Business Scenario:** This pipeline simulates a retail company consolidating sales data from multiple sources (batch CSV files from legacy systems, real-time Kafka events from point-of-sale terminals) into a unified analytics platform.

**Data Flow:**

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                 │
├──────────────────────────────────────────────────────────────────────┤
│  Batch: Legacy ERP CSV exports (daily sales reports)                 │
│  Streaming: Point-of-sale Kafka events (real-time transactions)      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data Lake)                       │
├──────────────────────────────────────────────────────────────────────┤
│  AWS S3 Storage with date partitioning (year=YYYY/month=MM/day=DD/)  │
│  Purpose: Immutable audit trail, regulatory compliance (90 days)     │
│  Format: CSV (batch), JSON (streaming)                               │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                          ┌──────────────┐
                          │  Spark ETL   │
                          │  Validation  │
                          │  Cleaning    │
                          │  Deduplication│
                          └──────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                SILVER LAYER (Validated Data Foundation)               │
├──────────────────────────────────────────────────────────────────────┤
│  Delta Lake 3.2.0 (ACID transactions, schema enforcement)            │
│  Purpose: Single source of truth for downstream analytics            │
│  Features: Time travel (30 days), automatic file management          │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ (Dataset Trigger)
                          ┌──────────────┐
                          │ dbt Models   │
                          │ Staging      │
                          │ Intermediate │
                          │ Marts (SCD2) │
                          └──────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Business Analytics)                      │
├──────────────────────────────────────────────────────────────────────┤
│  Snowflake Cloud Data Warehouse (Star Schema)                        │
│  Dimensions: dim_customer, dim_product, dim_store, dim_date          │
│  Facts: fact_sales (with data quality scores)                        │
│  Purpose: Business intelligence, reporting, ad-hoc analysis          │
└──────────────────────────────────────────────────────────────────────┘
```

**Business Value:**
- **Unified View:** Combines batch and streaming sources into single source of truth
- **Historical Tracking:** SCD Type 2 dimensions track changes over time (customer relocations, product price changes)
- **Data Quality:** Automated testing and quality scores ensure analytics reliability
- **Compliance:** Bronze layer provides immutable audit trail for regulatory requirements
- **Scalability:** Cloud-native architecture scales from gigabytes to petabytes

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

# 3. Start Services & Upload Data
docker-compose up -d
python src/bronze/data_uploader.py --source data/sample_sales.csv
```

**Next Steps:**
- Access **Airflow UI**: http://localhost:8080 (credentials: admin/admin)
- Trigger `batch_processing_dag` to process data through Bronze → Silver → Gold
- **[Complete Tutorial](docs/tutorial/getting-started.md)** - 60-minute guided walkthrough
- **[Configure Environment](docs/how-to/configure-environment.md)** - Detailed local setup

## Usage

### Batch Processing (CSV Files)

```bash
# Upload CSV to Bronze layer
python src/bronze/data_uploader.py --source data/sample_sales.csv

# Trigger Airflow DAG via UI
# 1. Open http://localhost:8080
# 2. Enable and trigger 'batch_processing_dag'
# 3. Monitor 'analytics_dag' (auto-triggered)
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

### Top 3 Common Issues

**1. Airflow Webserver Not Starting**
```bash
docker-compose logs airflow-webserver
# Wait 2-3 minutes for initialization (database migration on first startup)
```

**2. S3 Access Denied**
```bash
aws sts get-caller-identity  # Verify AWS credentials
aws s3 ls s3://YOUR_RAW_BUCKET/  # Test bucket access
```

**3. Snowflake Connection Failed**
```bash
# Verify key-pair authentication
python tools/validation/config_validator.py --validate-snowflake
```

**Complete Troubleshooting:** [Troubleshooting Guide](docs/how-to/troubleshooting.md) covers 20+ issues including dbt test failures, Delta Lake errors, Kafka Connect problems, and Docker issues.

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

## Contributing

Contributions are welcome! This project follows standard open-source practices.

**How to Contribute:**

1. **Fork the repository** and create a feature branch
2. **Make your changes** with appropriate tests and documentation
3. **Run the test suite** to ensure all tests pass
4. **Submit a pull request** with a clear description of your changes

**Contribution Guidelines:**

- Follow existing code style (Black, Flake8, MyPy for Python)
- Add tests for new functionality (maintain 90%+ coverage)
- Update documentation in `docs/` for user-facing changes
- Use professional tone (no emojis or marketing language)
- Ensure all CI/CD checks pass

**Reporting Issues:**

- Use GitHub Issues for bug reports and feature requests
- Provide detailed reproduction steps for bugs
- Include environment details (OS, Python version, Docker version)

**Questions?** Open a discussion on GitHub or create an issue.

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
