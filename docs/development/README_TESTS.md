# Spark ETL Test Suite

This directory contains a comprehensive test suite for the Spark ETL job (`sales_batch_job.py`) that processes sales data and loads it into Snowflake.

## Overview

The test suite provides comprehensive coverage for:
- **Unit Tests**: Fast tests with no external dependencies
- **Integration Tests**: Tests with real Snowflake/S3/Spark connectivity
- **Performance Tests**: Benchmarking and stress tests
- **Data Quality Tests**: Validation of data transformations and business rules
- **Error Handling Tests**: Edge cases and failure scenarios
- **Configuration Tests**: Environment-specific settings and validation

## Test Files

| File | Description |
|------|-------------|
| `conftest.py` | Pytest fixtures and test utilities |
| `test_sales_etl_job.py` | Unit tests for SalesETLJob class methods |
| `test_data_quality.py` | Data quality validation and business rule tests |
| `test_integration.py` | Integration tests with Snowflake, S3, and Spark environments |
| `test_performance.py` | Performance benchmarks and stress tests |
| `test_error_handling.py` | Error handling and edge case tests |
| `test_config.py` | Configuration management and environment tests |
| `run_tests.py` | Test runner with multiple execution modes |
| `pytest.ini` | Pytest configuration |

## Quick Start

### Prerequisites

1. **Install Dependencies**:
   ```bash
   pip install -r requirements-test.txt
   ```

2. **Optional: Set up credentials** (for integration tests):
   ```bash
   export SNOWFLAKE_ACCOUNT="your-account.snowflakecomputing.com"
   export SNOWFLAKE_USER="your-username"
   export SNOWFLAKE_PASSWORD="your-password"
   export AWS_ACCESS_KEY_ID="your-aws-key"
   export AWS_SECRET_ACCESS_KEY="your-aws-secret"
   ```

### Running Tests

#### Basic Usage
```bash
# Run unit tests (fast, no external dependencies)
python run_tests.py unit

# Run all tests
python run_tests.py all

# Run with coverage reporting
python run_tests.py coverage
```

#### Advanced Usage
```bash
# Run integration tests (requires credentials)
python run_tests.py integration

# Run performance tests
python run_tests.py performance

# Run specific test pattern
python run_tests.py -k "test_data_cleaning"

# Run specific test file
python run_tests.py -f test_sales_etl_job.py

# Validate environment setup
python run_tests.py validate

# List all available tests
python run_tests.py --list-tests
```

#### Direct Pytest Usage
```bash
# Run unit tests only
pytest -m "not integration and not slow"

# Run integration tests
pytest -m integration

# Run performance tests
pytest -m performance

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test
pytest test_sales_etl_job.py::TestSalesETLJob::test_clean_and_transform_valid_data
```

## Test Categories

### Unit Tests (`-m "not integration and not slow"`)
- **Fast execution** (< 5 seconds each)
- **No external dependencies** (mocked Snowflake, S3, etc.)
- **High coverage** of core ETL logic
- **Safe to run anywhere** (CI/CD, local development)

**Key Test Areas**:
- Schema validation and data type checking
- Data cleaning and transformation logic
- Business rule enforcement
- Error handling for invalid data
- Configuration validation

### Integration Tests (`-m integration`)
- **Require external services** (Snowflake, S3, Spark)
- **Validate end-to-end workflows**
- **Test real connectivity and authentication**
- **Slower execution** (30 seconds to several minutes)

**Key Test Areas**:
- Snowflake connection and data writing
- S3 file reading and writing
- Cross-platform Spark compatibility
- Network error handling
- Authentication and authorization

### Performance Tests (`-m performance`)
- **Benchmark ETL performance** across data sizes
- **Memory usage monitoring**
- **Scalability validation**
- **Regression detection**

**Key Test Areas**:
- Processing rates (records/second)
- Memory efficiency and leak detection
- Spark optimization effectiveness
- Large dataset handling

### Data Quality Tests
- **Business rule validation**
- **Data consistency checks**
- **Schema compliance verification**
- **Duplicate detection**

**Key Test Areas**:
- Null value handling
- Data range validation
- Categorical value verification
- Calculation accuracy (unit price, profit margin)

## Environment Setup

### Local Development
```bash
# Minimal setup for unit tests
pip install pytest pyspark pandas faker

# Run unit tests
python run_tests.py unit
```

### Full Integration Testing
```bash
# Install all dependencies
pip install -r requirements-test.txt

# Set environment variables
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export TEST_S3_BUCKET="your-test-bucket"

# Run all tests
python run_tests.py all
```

### Databricks Environment
```python
# In Databricks notebook
%pip install -r requirements-test.txt

# Set Databricks-specific environment
import os
os.environ["DATABRICKS_HOST"] = "your-workspace-url"
os.environ["DATABRICKS_TOKEN"] = "your-token"

# Run tests
%run ./run_tests.py unit
```

## Test Data

The test suite uses multiple data generation strategies:

### Sample Data (conftest.py)
- **Realistic sales data** with proper business relationships
- **Configurable size** (100-50,000 records)
- **Multiple complexity levels** (simple/complex)
- **Consistent and reproducible** (seeded random generation)

### Edge Case Data
- **Null values** in critical fields
- **Invalid data types** and formats
- **Extreme numeric values**
- **Malformed dates and strings**
- **Boundary conditions** (zero, negative values)

### Performance Data
- **Large datasets** (10K-50K records)
- **Memory stress scenarios** (large strings, many columns)
- **Multi-file processing** (1-10 files)

## Configuration Testing

### Environment-Specific Configs
- **Local**: `local[*]` master, minimal memory
- **Databricks**: Cloud-optimized settings
- **EMR**: Dynamic allocation, AWS packages
- **Production**: High-performance settings

### Snowflake Configuration
- **Connection validation**
- **Authentication testing**
- **Database/schema/warehouse settings**
- **Error handling for invalid credentials**

## Performance Benchmarks

### Expected Performance (Local Environment)
- **Small datasets (1K records)**: < 5 seconds
- **Medium datasets (10K records)**: < 30 seconds
- **Large datasets (50K records)**: < 5 minutes
- **Memory usage**: < 1MB per 1000 records

### Monitored Metrics
- **Processing rate**: Records per second
- **Memory efficiency**: Peak memory usage
- **Cleanup effectiveness**: Memory release after processing
- **Spark optimization**: Partition and cache utilization

## Troubleshooting

### Common Issues

#### "PySpark not available"
```bash
# Install PySpark
pip install pyspark

# Or use conda
conda install pyspark
```

#### "Snowflake credentials not available"
```bash
# Set required environment variables
export SNOWFLAKE_ACCOUNT="account.region.cloud"
export SNOWFLAKE_USER="username"
export SNOWFLAKE_PASSWORD="password"
```

#### "AWS credentials not available"
```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

# Or use AWS CLI
aws configure
```

#### Test timeouts
```bash
# Increase timeout for slow tests
pytest --timeout=600  # 10 minutes

# Skip slow tests
pytest -m "not slow"
```

#### Memory issues
```bash
# Reduce test data size in conftest.py
# Or increase available memory
export SPARK_DRIVER_MEMORY="4g"
```

### Debug Mode
```bash
# Run with verbose output
pytest -v -s

# Run single test with debugging
pytest test_sales_etl_job.py::test_specific_function -v -s

# Enable Spark debug logging
export SPARK_LOG_LEVEL="DEBUG"
```

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Spark ETL Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.8
      - name: Install dependencies
        run: |
          pip install -r requirements-test.txt
      - name: Run unit tests
        run: |
          cd spark && python run_tests.py unit
      - name: Run integration tests
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          cd spark && python run_tests.py integration
```

### Jenkins Pipeline Example
```groovy
pipeline {
    agent any
    stages {
        stage('Test') {
            steps {
                sh 'pip install -r spark/requirements-test.txt'
                sh 'cd spark && python run_tests.py unit'
            }
        }
        stage('Integration') {
            when { branch 'main' }
            steps {
                withCredentials([...]) {
                    sh 'cd spark && python run_tests.py integration'
                }
            }
        }
    }
    post {
        always {
            publishHTML([
                allowMissing: false,
                alwaysLinkToLastBuild: true,
                keepAll: true,
                reportDir: 'spark/htmlcov',
                reportFiles: 'index.html',
                reportName: 'Coverage Report'
            ])
        }
    }
}
```

## Contributing

### Adding New Tests

1. **Choose appropriate test file**:
   - `test_sales_etl_job.py`: Core ETL functionality
   - `test_data_quality.py`: Data validation and business rules
   - `test_integration.py`: External service integration
   - `test_performance.py`: Performance and benchmarking
   - `test_error_handling.py`: Error scenarios
   - `test_config.py`: Configuration management

2. **Use appropriate markers**:
   ```python
   @pytest.mark.unit
   @pytest.mark.integration
   @pytest.mark.performance
   @pytest.mark.slow
   ```

3. **Follow naming conventions**:
   ```python
   def test_specific_functionality_with_clear_description():
       """Test description explaining what is being validated."""
   ```

4. **Use fixtures from conftest.py**:
   ```python
   def test_my_function(spark_session, sample_sales_data, etl_job_with_mock_snowflake):
       # Test implementation
   ```

### Test Quality Guidelines

- **Clear test names** that describe what is being tested
- **Comprehensive assertions** that validate expected behavior
- **Proper cleanup** of resources (Spark sessions, temp files)
- **Mock external dependencies** for unit tests
- **Performance assertions** for benchmark tests
- **Documentation** for complex test scenarios

## License

This test suite is part of the Sales Data Aggregation Pipeline project.
