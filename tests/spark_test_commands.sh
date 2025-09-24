#!/bin/bash
# Test command shortcuts for Spark ETL testing
# Usage: source test_commands.sh

# Basic test commands
alias test-unit="python tests/run_spark_tests.py unit"
alias test-all="python tests/run_spark_tests.py all"
alias test-integration="python tests/run_spark_tests.py integration"
alias test-performance="python tests/run_spark_tests.py performance"
alias test-coverage="python tests/run_spark_tests.py coverage"

# Environment validation
alias test-validate="python tests/run_spark_tests.py validate"
alias test-summary="python tests/run_spark_tests.py summary"

# Specific test categories
alias test-etl="pytest test_sales_etl_job.py -v"
alias test-quality="pytest test_data_quality.py -v"
alias test-config="pytest test_config.py -v"
alias test-errors="pytest test_error_handling.py -v"

# Development helpers
alias test-fast="pytest -m 'not slow and not integration' --tb=short"
alias test-quick="pytest -x --tb=short"  # Stop on first failure
alias test-verbose="pytest -v -s"
alias test-debug="pytest -v -s --pdb"  # Drop into debugger on failure

# Coverage commands
alias test-cov="pytest --cov=. --cov-report=html --cov-report=term-missing"
alias test-cov-view="open htmlcov/index.html"  # macOS
alias test-cov-view-linux="xdg-open htmlcov/index.html"  # Linux

# Clean up commands
alias test-clean="rm -rf .pytest_cache __pycache__ .coverage htmlcov"
alias test-clean-logs="rm -rf spark-warehouse-* derby.log metastore_db"

# Installation helpers
alias test-deps="pip install -r requirements-test.txt"
alias test-deps-min="pip install pytest pyspark pandas faker"

# Environment setup helpers
test-env-check() {
    echo "Checking test environment..."
    python -c "
import sys
print(f'Python: {sys.version}')

try:
    import pyspark
    print('SUCCESS: PySpark available')
except ImportError:
    print('ERROR: PySpark not available')

try:
    import pytest
    print('SUCCESS: pytest available')
except ImportError:
    print('ERROR: pytest not available')

try:
    import snowflake.connector
    print('SUCCESS: Snowflake connector available')
except ImportError:
    print('ERROR: Snowflake connector not available')

import os
sf_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
sf_configured = all(os.getenv(var) for var in sf_vars)
print(f'{"SUCCESS:" if sf_configured else "ERROR:"} Snowflake credentials configured')

aws_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
aws_configured = all(os.getenv(var) for var in aws_vars)
print(f'{"SUCCESS:" if aws_configured else "ERROR:"} AWS credentials configured')
"
}

test-env-setup() {
    echo "Setting up test environment..."

    # Install dependencies
    echo "Installing test dependencies..."
    pip install -r requirements-test.txt

    # Check for credentials
    if [[ -z "$SNOWFLAKE_ACCOUNT" ]]; then
        echo "WARNING: SNOWFLAKE_ACCOUNT not set. Integration tests will be limited."
    fi

    if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
        echo "WARNING: AWS credentials not set. S3 integration tests will be limited."
    fi

    echo "SUCCESS: Test environment setup complete!"
}

# Test data generators
test-data-sample() {
    python -c "
from tests.conftest_spark import generate_sample_data
import pandas as pd
data = generate_sample_data(100)
df = pd.DataFrame(data)
print('Sample data generated:')
print(df.head())
print(f'Total records: {len(df)}')
df.to_csv('sample_test_data.csv', index=False)
print('Saved to sample_test_data.csv')
"
}

# Performance testing helpers
test-benchmark() {
    echo "🏃 Running performance benchmarks..."
    pytest -m benchmark --tb=short
}

test-memory() {
    echo "🧠 Running memory tests..."
    pytest test_performance.py::TestMemoryUsage -v
}

# Integration test helpers
test-snowflake() {
    echo "Testing Snowflake integration..."
    pytest -m snowflake --tb=short
}

test-aws() {
    echo "Testing AWS integration..."
    pytest -m aws --tb=short
}

# Utility functions
test-list() {
    echo "📋 Available tests:"
    pytest --collect-only -q
}

test-help() {
    echo "🧪 Spark ETL Test Commands Help"
    echo "================================"
    echo ""
    echo "Basic Commands:"
    echo "  test-unit          - Run unit tests (fast, no external deps)"
    echo "  test-all           - Run all tests"
    echo "  test-integration   - Run integration tests (requires credentials)"
    echo "  test-performance   - Run performance tests"
    echo "  test-coverage      - Run tests with coverage reporting"
    echo ""
    echo "Validation:"
    echo "  test-validate      - Validate test environment"
    echo "  test-summary       - Show test summary"
    echo "  test-env-check     - Check environment setup"
    echo "  test-env-setup     - Set up test environment"
    echo ""
    echo "Specific Tests:"
    echo "  test-etl           - Test ETL job functionality"
    echo "  test-quality       - Test data quality validation"
    echo "  test-config        - Test configuration management"
    echo "  test-errors        - Test error handling"
    echo ""
    echo "Development:"
    echo "  test-fast          - Run fast tests only"
    echo "  test-quick         - Stop on first failure"
    echo "  test-verbose       - Verbose output"
    echo "  test-debug         - Debug mode (drop to pdb on failure)"
    echo ""
    echo "Coverage:"
    echo "  test-cov           - Generate coverage report"
    echo "  test-cov-view      - Open coverage report in browser"
    echo ""
    echo "Cleanup:"
    echo "  test-clean         - Clean test artifacts"
    echo "  test-clean-logs    - Clean Spark logs"
    echo ""
    echo "Utilities:"
    echo "  test-list          - List all available tests"
    echo "  test-data-sample   - Generate sample test data"
    echo "  test-benchmark     - Run performance benchmarks"
    echo "  test-memory        - Run memory tests"
    echo "  test-snowflake     - Test Snowflake integration"
    echo "  test-aws           - Test AWS integration"
    echo ""
    echo "Examples:"
    echo "  test-unit                                    # Run unit tests"
    echo "  pytest -k 'test_data_cleaning' -v          # Run specific test"
    echo "  pytest test_sales_etl_job.py::TestSalesETLJob::test_init -v  # Run specific test method"
    echo "  pytest -m 'not slow' --tb=short            # Skip slow tests"
}

echo "🧪 Spark ETL test commands loaded!"
echo "Run 'test-help' for available commands."
