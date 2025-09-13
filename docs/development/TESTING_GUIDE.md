# Sales Data Pipeline - Comprehensive Testing Guide

This guide provides step-by-step instructions for testing all components of the sales data aggregation pipeline.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Test Categories](#test-categories)
3. [Setup Instructions](#setup-instructions)
4. [Running Tests](#running-tests)
5. [Test Validation Procedures](#test-validation-procedures)
6. [Troubleshooting](#troubleshooting)
7. [Continuous Integration](#continuous-integration)

## Prerequisites

### Required Software
- Docker Desktop (latest version)
- Python 3.9+ with pip
- Git
- AWS CLI configured (for cloud tests)

### Required Python Packages
```bash
pip install -r requirements.txt
pip install pytest pytest-cov docker kafka-python psycopg2-binary boto3 requests snowflake-connector-python databricks-sql-connector
```

### Environment Setup
```bash
# Copy and configure environment file
cp .env.example .env
# Edit .env with your specific configuration
```

## Test Categories

### 1. Docker Container Health Tests (`test_docker_health.py`)
- **Purpose**: Validate Docker containers are running and healthy
- **Scope**: Container status, resource usage, network connectivity
- **Duration**: ~2-3 minutes

### 2. Configuration Validation Tests (`test_config_validation.py`)
- **Purpose**: Validate environment configuration and credentials
- **Scope**: .env file, AWS credentials, database connections
- **Duration**: ~3-5 minutes

### 3. Service Readiness Tests (`test_service_readiness.py`)
- **Purpose**: Validate service startup and initialization
- **Scope**: Service health checks, dependency validation
- **Duration**: ~5-10 minutes

### 4. Inter-Service Communication Tests (`test_inter_service_communication.py`)
- **Purpose**: Validate communication between services
- **Scope**: API endpoints, database connections, data flow
- **Duration**: ~3-5 minutes

### 5. Kafka Streaming Tests (`test_kafka_streaming.py`)
- **Purpose**: Validate Kafka producer, consumer, and streaming functionality
- **Scope**: Message production/consumption, partitioning, serialization
- **Duration**: ~5-7 minutes

### 6. ETL Transform Tests (`test_transform.py`)
- **Purpose**: Validate data transformation logic
- **Scope**: Data cleaning, validation, formatting
- **Duration**: ~1-2 minutes

## Setup Instructions

### 1. Initial Environment Setup

```bash
# Navigate to project directory
cd sales_data_aggregation_pipeline

# Copy environment template
cp .env.example .env

# Edit .env file with your configuration
# Key variables to configure:
# - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
# - S3_BUCKET, PROCESSED_BUCKET
# - RDS_HOST, RDS_USER, RDS_PASS, RDS_DB
# - HOST_DATA_DIR (absolute path to your data directory)
# - ALLOWED_CIDR (your public IP with /32)
```

### 2. Build Docker Images

```bash
# Build the ETL pipeline image
docker build -t sales-pipeline:latest .

# Build Airflow image
docker build -f Dockerfile-airflow -t custom-airflow:2.6.1 .
```

### 3. Prepare Test Data

```bash
# Ensure sample CSV files exist in data/raw/ directory
mkdir -p data/raw
# Copy your CSV files to data/raw/ (files should be named *_orders.csv)
```

### 4. Start Infrastructure

```bash
# Start core services (without Kafka for basic tests)
docker-compose up -d postgres etl-pipeline airflow

# For Kafka tests, start Kafka services
docker-compose --profile kafka-local up -d zookeeper kafka kafka-ui

# Wait for services to be ready (this may take 3-5 minutes)
```

## Running Tests

### Quick Health Check
```bash
# Run basic connectivity tests
python -m pytest tests/test_docker_health.py::test_docker_compose_services_running -v
python -m pytest tests/test_config_validation.py::test_env_file_exists -v
```

### Full Test Suite

#### 1. Configuration Tests (Run First)
```bash
# Test environment configuration
python -m pytest tests/test_config_validation.py -v

# Expected results:
# ✓ Environment file exists
# ✓ Core environment variables are set
# ✓ AWS credentials are valid
# ✓ S3 buckets are accessible
# ✓ Database connections work
```

#### 2. Docker Health Tests
```bash
# Test Docker containers
python -m pytest tests/test_docker_health.py -v

# Expected results:
# ✓ All expected services are running
# ✓ Services are healthy and responsive
# ✓ Resource usage is within limits
# ✓ Network connectivity works
```

#### 3. Service Readiness Tests
```bash
# Test service startup and readiness
python -m pytest tests/test_service_readiness.py -v

# Expected results:
# ✓ PostgreSQL is ready
# ✓ Airflow is ready and accessible
# ✓ Services start in correct order
# ✓ Dependencies are satisfied
```

#### 4. Inter-Service Communication Tests
```bash
# Test service communication
python -m pytest tests/test_inter_service_communication.py -v

# Expected results:
# ✓ Service ports are accessible
# ✓ Airflow can communicate with Docker
# ✓ Database connections work
# ✓ API endpoints respond correctly
```

#### 5. Kafka Streaming Tests (If Kafka is enabled)
```bash
# Test Kafka functionality
python -m pytest tests/test_kafka_streaming.py -v

# Expected results:
# ✓ Kafka producer sends messages
# ✓ Kafka consumer receives messages
# ✓ Partitioning works correctly
# ✓ Consumer groups balance load
# ✓ Serialization formats work
```

#### 6. ETL Transform Tests
```bash
# Test ETL transformations
python -m pytest tests/test_transform.py -v

# Expected results:
# ✓ Date parsing works
# ✓ Data cleaning removes invalid records
# ✓ Field derivation calculates correctly
# ✓ Column renaming works
```

### Running All Tests Together
```bash
# Run complete test suite with coverage
python -m pytest tests/ -v --cov=. --cov-report=html

# Run specific test categories
python -m pytest tests/test_docker_health.py tests/test_config_validation.py -v

# Run tests with specific markers (if you add pytest markers)
python -m pytest -m "not slow" -v
```

## Test Validation Procedures

### Manual Validation Steps

#### 1. Verify Docker Environment
```bash
# Check running containers
docker ps

# Expected containers:
# - postgres (port 5432)
# - airflow (port 8080)
# - etl-pipeline
# - kafka (port 9092) [if using Kafka profile]
# - zookeeper (port 2181) [if using Kafka profile]
# - kafka-ui (port 8090) [if using Kafka profile]

# Check container logs for errors
docker logs <container_name>
```

#### 2. Verify Service Accessibility
```bash
# Test PostgreSQL
psql -h localhost -p 5432 -U airflow -d airflow -c "SELECT 1;"

# Test Airflow Web UI
curl -u admin:admin http://localhost:8080/health

# Test Kafka (if running)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

#### 3. Verify Data Directories
```bash
# Check data directory structure
ls -la data/
ls -la data/raw/

# Should contain CSV files like:
# - Asia_Pacific_orders.csv
# - Europe_orders.csv
# - North_America_orders.csv
```

#### 4. Verify AWS Connectivity
```bash
# Test S3 access
aws s3 ls s3://your-raw-bucket-name/

# Test RDS connectivity (if configured)
psql -h your-rds-endpoint -U salesuser -d sales -c "SELECT version();"
```

### Automated Validation

#### Health Check Script
```bash
#!/bin/bash
# Create a health check script

echo "=== Pipeline Health Check ==="

# Check Docker services
echo "Checking Docker services..."
docker-compose ps

# Run critical tests
echo "Running critical tests..."
python -m pytest tests/test_docker_health.py::test_docker_compose_services_running
python -m pytest tests/test_config_validation.py::test_core_environment_variables
python -m pytest tests/test_service_readiness.py::test_postgres_startup_and_readiness

echo "Health check complete!"
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Docker Containers Not Starting
```bash
# Check Docker daemon
docker --version
docker info

# Check available resources
docker system df
docker system prune  # If low on space

# Rebuild images
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

#### 2. Port Conflicts
```bash
# Check port usage
netstat -an | grep ":8080\|:5432\|:9092"

# Stop conflicting services
# For Windows: Stop services using Task Manager or Services app
# For Linux/Mac: sudo lsof -i :8080 && kill -9 <PID>
```

#### 3. Environment Configuration Issues
```bash
# Validate .env file
cat .env | grep -v "^#" | grep "="

# Test individual components
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('AWS Key:', bool(os.getenv('AWS_ACCESS_KEY_ID')))"

# Test AWS credentials
aws sts get-caller-identity
```

#### 4. Kafka Connection Issues
```bash
# Check Kafka broker
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Zookeeper
docker exec zookeeper zkCli.sh ls /

# Reset Kafka data (CAUTION: This deletes all Kafka data)
docker-compose down
docker volume prune
docker-compose --profile kafka-local up -d
```

#### 5. Database Connection Issues
```bash
# Check PostgreSQL logs
docker logs postgres

# Test connection
docker exec postgres psql -U airflow -d airflow -c "SELECT version();"

# Reset database (CAUTION: This deletes all data)
docker-compose down
docker volume rm sales_data_aggregation_pipeline_postgres_data
docker-compose up -d postgres
```

### Log Analysis

#### Key Log Locations
```bash
# Airflow logs
docker logs airflow
# Also check: ./airflow/logs/

# Postgres logs
docker logs postgres

# Kafka logs
docker logs kafka
docker logs zookeeper

# ETL container logs
docker logs etl-pipeline
```

#### Common Error Patterns
- **Connection refused**: Service not ready, check startup order
- **Permission denied**: Check file permissions and Docker socket access
- **No such file or directory**: Check HOST_DATA_DIR configuration
- **Invalid credentials**: Check AWS credentials and database passwords
- **Port already in use**: Check for conflicting services

## Continuous Integration

### GitHub Actions Setup
The project includes a CI pipeline in `.github/workflows/main.yml` that:
1. Runs linting (Black, Flake8)
2. Runs type checking (MyPy)
3. Runs the test suite
4. Builds Docker images

### Local CI Simulation
```bash
# Run the same checks as CI
black --check .
flake8 .
mypy src/
python -m pytest tests/ --cov=.
```

### Test Coverage Goals
- **Unit Tests**: >80% coverage for ETL functions
- **Integration Tests**: All service communication paths
- **End-to-End Tests**: Complete data pipeline flow
- **Configuration Tests**: All environment variables validated

### Performance Benchmarks
- **Container Startup**: <5 minutes for all services
- **Test Execution**: <15 minutes for full suite
- **Memory Usage**: <4GB total for all containers
- **CPU Usage**: <80% during normal operation

## Best Practices

### Test Development
1. **Isolation**: Each test should be independent
2. **Cleanup**: Always clean up test data and resources
3. **Deterministic**: Tests should be repeatable
4. **Fast Feedback**: Critical tests should run quickly
5. **Clear Assertions**: Test failures should be easy to diagnose

### Test Execution
1. **Environment**: Always use clean environment for critical tests
2. **Dependencies**: Start dependencies before running tests
3. **Monitoring**: Monitor resource usage during tests
4. **Documentation**: Keep test results and logs for debugging

### Maintenance
1. **Regular Updates**: Update test data and scenarios regularly
2. **Monitoring**: Monitor test execution times and failure rates
3. **Cleanup**: Regular cleanup of test artifacts and logs
4. **Documentation**: Keep testing guide updated with changes

---

## Quick Reference Commands

```bash
# Start minimal environment
docker-compose up -d postgres etl-pipeline airflow

# Start with Kafka
docker-compose --profile kafka-local up -d

# Run all tests
python -m pytest tests/ -v

# Run specific test category
python -m pytest tests/test_docker_health.py -v

# Check service health
curl http://localhost:8080/health  # Airflow
docker ps  # Container status

# Stop all services
docker-compose down

# Clean up everything
docker-compose down -v
docker system prune -f
```

For additional help, check the project's main README.md or consult the CLAUDE.md file for development guidance.
