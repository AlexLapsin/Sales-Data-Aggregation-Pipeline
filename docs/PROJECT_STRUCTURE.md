# Project Structure

This document describes the organized structure of the Sales Data Aggregation Pipeline after professional reorganization.

## Directory Structure Overview

```
sales_data_aggregation_pipeline/
├── 📁 src/                          # Core application source code
├── 📁 orchestration/                # Workflow orchestration (Airflow, scripts)
├── 📁 infrastructure/               # Infrastructure as Code (Terraform, Docker)
├── 📁 config/                       # Centralized configuration management
├── 📁 tests/                        # Unified testing framework
├── 📁 docs/                         # Consolidated documentation
├── 📁 tools/                        # Development and operational tools
├── 📁 requirements/                 # Dependency management
├── 📄 .env                          # Environment configuration (user's original)
├── 📄 .env.example                  # Environment template
├── 📄 README.md                     # Main project documentation
├── 📄 pyproject.toml                # Modern Python project configuration
├── 📄 requirements.txt              # Main requirements file
└── 📄 docker-compose.yml            # Main compose configuration
```

## Detailed Structure

### 📁 src/ - Core Application
```
src/
├── __init__.py
├── etl/                            # ETL business logic
│   ├── __init__.py
│   ├── extract.py                  # Data extraction functions
│   ├── transform.py                # Data transformation functions
│   └── load.py                     # Data loading functions
├── streaming/                      # Kafka streaming components
│   ├── __init__.py
│   ├── producers.py                # Kafka producers
│   └── connectors/                 # Kafka connectors
│       └── __init__.py
├── spark/                          # Spark ETL jobs
│   ├── __init__.py
│   ├── config.py                   # Spark configuration
│   └── jobs/                       # Spark job definitions
│       ├── __init__.py
│       ├── batch_etl.py            # Main batch ETL job
│       └── notebooks/              # Databricks notebooks
├── dbt/                            # dbt transformations
│   ├── dbt_project.yml
│   ├── models/
│   ├── macros/
│   └── tests/
└── utils/                          # Shared utilities and helpers
    ├── __init__.py
    ├── config_validator.py         # Configuration validation
    ├── config_templates.py         # Configuration templates
    └── setup_doctor.py             # Setup and health checks
```

### 📁 orchestration/ - Workflow Management
```
orchestration/
├── airflow/                        # Airflow components
│   ├── dags/                       # DAG definitions
│   │   ├── sales_data_pipeline_dag.py
│   │   ├── cloud_sales_pipeline_dag.py
│   │   ├── maintenance_dag.py
│   │   └── pipeline_monitoring_dag.py
│   ├── plugins/                    # Custom plugins
│   ├── logs/                       # Airflow logs
│   └── postgres/                   # Postgres data
└── scripts/                        # PostgreSQL Pipeline scripts
    ├── __init__.py
    ├── postgres_preflight_check.py
    ├── postgres_create_tables.py
    ├── postgres_transform.py
    ├── postgres_load.py
    └── postgres_upload_data.py
```

### 📁 infrastructure/ - Infrastructure as Code
```
infrastructure/
├── terraform/                      # Terraform configurations
│   ├── modules/                    # Reusable Terraform modules
│   │   ├── storage/
│   │   ├── database/
│   │   ├── network/
│   │   └── iam/
│   └── environments/               # Environment-specific configs
├── docker/                         # Docker configurations
│   ├── airflow/
│   │   └── Dockerfile-airflow
│   └── etl/
│       └── Dockerfile
└── deployment/                     # Deployment scripts and configs
    └── scripts/
```

### 📁 tests/ - Unified Testing Framework
```
tests/
├── __init__.py
├── conftest.py                     # Shared pytest configuration
├── unit/                           # Unit tests organized by module
│   ├── __init__.py
│   ├── test_spark/                 # Spark unit tests
│   ├── test_streaming/             # Streaming unit tests
│   ├── test_spark/                 # Spark unit tests
│   ├── test_dbt/                   # dbt unit tests
│   └── test_airflow/               # Airflow unit tests
├── integration/                    # Integration tests
│   ├── __init__.py
│   ├── test_e2e_pipeline.py
│   ├── data_validators.py
│   ├── infrastructure_manager.py
│   └── performance_monitor.py
├── e2e/                           # End-to-end tests
│   └── __init__.py
├── performance/                    # Performance tests
│   └── __init__.py
├── fixtures/                       # Test fixtures and data
└── utils/                          # Testing utilities
```

### 📁 config/ - Configuration Management
```
config/
├── __init__.py
├── environments/                   # Environment-specific configs
├── platforms/                      # Platform-specific configs
│   ├── aws/
│   ├── gcp/
│   └── azure/
└── templates/                      # Configuration templates
```

### 📁 docs/ - Documentation
```
docs/
├── README.md                       # Documentation overview
├── architecture/                   # System architecture docs
│   ├── snowflake.md
│   └── streaming.md
├── deployment/                     # Deployment guides
│   └── cloud-deployment.md
├── development/                    # Development guides
│   ├── TESTING_GUIDE.md
│   ├── CONFIG_VALIDATOR_README.md
│   └── KAFKA_PRODUCER_TESTS_SUMMARY.md
├── api/                           # API documentation
└── troubleshooting/               # Troubleshooting guides
```

### 📁 tools/ - Development Tools
```
tools/
├── __init__.py
├── testing/                        # Testing tools and runners
│   ├── __init__.py
│   ├── run_tests.py                # Main test runner
│   └── test_runners/
├── validation/                     # Validation tools
│   ├── __init__.py
│   └── demo_config_validator.py
└── monitoring/                     # Monitoring tools
    └── __init__.py
```

### 📁 requirements/ - Dependency Management
```
requirements/
├── base.txt                        # Core dependencies
├── dev.txt                         # Development dependencies
├── test.txt                        # Testing dependencies
├── cloud.txt                       # Cloud-specific dependencies
└── validation.txt                  # Validation tool dependencies
```

## Key Changes Made

### 1. **Root Directory Cleanup**
- Moved utility files from root to appropriate modules
- Only essential files remain at root level
- Created modern `pyproject.toml` configuration

### 2. **Source Code Organization**
- Renamed files to follow Python conventions (`extract.py` vs `extract_funcs.py`)
- Organized by business domain (ETL, streaming, spark, etc.)
- Created proper package structure with `__init__.py` files

### 3. **Testing Consolidation**
- Unified all tests under single `tests/` directory
- Organized by test type (unit, integration, e2e, performance)
- Proper pytest configuration and fixtures

### 4. **Infrastructure Organization**
- Separated infrastructure code from application code
- Organized Docker files by service
- Grouped deployment scripts and configurations

### 5. **Configuration Management**
- Centralized all configuration files
- Organized by environment and platform
- Created template system for easy setup

### 6. **Documentation Structure**
- Consolidated all documentation under `docs/`
- Organized by purpose (architecture, deployment, development)
- Created clear navigation structure

## Import Changes

Key import changes made during reorganization:

```python
# OLD
from etl.extract_funcs import get_data_files
from etl.transform_funcs import process_sales_data
from etl.load_funcs import load_to_postgres

# NEW
from src.etl.extract import get_data_files
from src.etl.transform import process_sales_data
from src.etl.load import load_to_postgres
```

```python
# OLD
from kafka_producer import SalesDataProducer

# NEW
from src.streaming.producers import SalesDataProducer
```

```python
# OLD
from sales_batch_job import SalesETLJob

# NEW
from src.spark.jobs.batch_etl import SalesETLJob
```

## Benefits of New Structure

1. **Professional Organization**: Follows Python packaging best practices
2. **Clear Separation of Concerns**: Each directory has a single responsibility
3. **Scalable Structure**: Easy to add new components without cluttering
4. **Better Testing**: Unified testing framework with clear organization
5. **Improved Documentation**: Consolidated and well-organized docs
6. **Modern Configuration**: Uses `pyproject.toml` and proper dependency management
7. **Infrastructure as Code**: Clear separation of infrastructure concerns
8. **Development Tools**: Dedicated space for development utilities

This structure provides a solid foundation for enterprise-scale development while maintaining clarity and ease of navigation.
