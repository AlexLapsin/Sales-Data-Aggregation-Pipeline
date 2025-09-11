# Airflow DAG Testing Suite

This directory contains comprehensive tests for all Airflow DAGs in the sales data aggregation pipeline. The test suite covers unit tests, integration tests, performance tests, and edge case scenarios.

## Test Structure

```
tests/airflow/
├── conftest.py                     # Pytest configuration and fixtures
├── test_utils.py                   # Test utilities and mock helpers
├── pytest.ini                     # Pytest configuration file
├── README.md                       # This documentation
├── test_cloud_sales_pipeline_dag.py    # Main pipeline DAG tests
├── test_pipeline_monitoring_dag.py     # Monitoring DAG tests
├── test_maintenance_dag.py             # Maintenance DAG tests
├── test_dag_integration.py             # Cross-DAG integration tests
├── test_dag_performance.py             # Performance and load tests
└── test_dag_edge_cases.py              # Edge cases and error scenarios
```

## Test Categories

### Unit Tests (`@pytest.mark.unit`)
- **Purpose**: Test individual components in isolation
- **Speed**: Fast (< 1 second per test)
- **Dependencies**: No external services required
- **Coverage**: DAG structure, task configuration, Python functions

### Integration Tests (`@pytest.mark.integration`)
- **Purpose**: Test component interactions and workflows
- **Speed**: Medium (1-10 seconds per test)
- **Dependencies**: Mocked external services
- **Coverage**: Cross-DAG dependencies, data flow, end-to-end scenarios

### Performance Tests (`@pytest.mark.slow`)
- **Purpose**: Test performance characteristics and scalability
- **Speed**: Slow (10+ seconds per test)
- **Dependencies**: Large datasets, concurrent execution
- **Coverage**: Memory usage, execution time, resource consumption

### External Service Tests (`@pytest.mark.external`)
- **Purpose**: Test integration with real external services
- **Speed**: Variable (depends on service response)
- **Dependencies**: Snowflake, Databricks, Kafka connections
- **Coverage**: Real service integration, authentication, connectivity

## Running Tests

### Prerequisites

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt -r requirements-dev.txt
   ```

2. **Set Environment Variables**:
   ```bash
   # Copy example environment file
   cp .env.example .env

   # Edit .env with your test configuration
   # Test environment variables are automatically loaded
   ```

3. **Airflow Setup**:
   ```bash
   # Initialize Airflow database (optional for testing)
   airflow db init
   ```

### Basic Test Execution

```bash
# Run all tests
pytest tests/airflow/

# Run specific test categories
pytest tests/airflow/ -m unit                    # Unit tests only
pytest tests/airflow/ -m integration             # Integration tests only
pytest tests/airflow/ -m "not slow"             # Exclude slow tests
pytest tests/airflow/ -m "unit or integration"   # Unit and integration tests

# Run specific test files
pytest tests/airflow/test_cloud_sales_pipeline_dag.py
pytest tests/airflow/test_pipeline_monitoring_dag.py
pytest tests/airflow/test_maintenance_dag.py

# Run specific test classes or functions
pytest tests/airflow/test_cloud_sales_pipeline_dag.py::TestDAGStructure
pytest tests/airflow/test_monitoring_dag.py::TestGenerateDailyReport::test_success
```

### Advanced Test Options

```bash
# Verbose output with detailed logs
pytest tests/airflow/ -v -s

# Run with coverage reporting
pytest tests/airflow/ --cov=tests/airflow --cov-report=html

# Run tests in parallel (requires pytest-xdist)
pytest tests/airflow/ -n auto

# Run only failed tests from last run
pytest tests/airflow/ --lf

# Run tests with specific patterns
pytest tests/airflow/ -k "kafka"              # Tests with 'kafka' in name
pytest tests/airflow/ -k "not performance"    # Exclude performance tests

# Run with timeout (requires pytest-timeout)
pytest tests/airflow/ --timeout=300           # 5 minute timeout per test

# Generate JUnit XML for CI/CD
pytest tests/airflow/ --junitxml=test-results.xml
```

### Performance Testing

```bash
# Run performance tests only
pytest tests/airflow/ -m slow

# Run with performance profiling
pytest tests/airflow/test_dag_performance.py --profile

# Run memory usage tests
pytest tests/airflow/test_dag_performance.py::TestMemoryUsage

# Run load testing scenarios
pytest tests/airflow/test_dag_performance.py::TestLoadTesting
```

## Test Configuration

### Environment Variables

The test suite uses these environment variables (automatically set in test mode):

```bash
# Airflow Configuration
AIRFLOW__CORE__UNIT_TEST_MODE=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False

# Test Database Configuration (uses test values)
SNOWFLAKE_ACCOUNT=test-account
SNOWFLAKE_USER=test_user
SNOWFLAKE_PASSWORD=test_password
SNOWFLAKE_DATABASE=TEST_SALES_DW
SNOWFLAKE_WAREHOUSE=TEST_COMPUTE_WH

# AWS Configuration (test values)
AWS_ACCESS_KEY_ID=test_access_key
AWS_SECRET_ACCESS_KEY=test_secret_key
S3_BUCKET=test-sales-bucket

# Other Service Configuration
DATABRICKS_HOST=https://test.databricks.com
DATABRICKS_TOKEN=test_token
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### Custom Pytest Markers

```python
# Mark tests with appropriate categories
@pytest.mark.unit
def test_dag_structure():
    pass

@pytest.mark.integration
def test_cross_dag_workflow():
    pass

@pytest.mark.slow
def test_performance_characteristics():
    pass

@pytest.mark.external
def test_real_snowflake_connection():
    pass
```

## Test Patterns and Examples

### Testing DAG Structure

```python
def test_dag_tasks_exist(cloud_sales_dag):
    """Test that all expected tasks exist"""
    expected_tasks = [
        "start_pipeline",
        "kafka_health_check",
        "spark_batch_processing",
        # ... more tasks
    ]
    assert_dag_structure(cloud_sales_dag, expected_tasks)

def test_task_dependencies(cloud_sales_dag):
    """Test task dependencies are correct"""
    assert_task_dependencies(
        cloud_sales_dag,
        "dbt_run_staging",
        ["dbt_deps"]
    )
```

### Testing Python Functions

```python
@patch('airflow.providers.snowflake.hooks.snowflake.SnowflakeHook')
def test_generate_daily_report(mock_hook_class):
    """Test daily report generation"""
    # Setup mock
    mock_hook = Mock()
    mock_hook.get_first.return_value = [20240101, 1000, 50000.0, 250, 85.5]
    mock_hook_class.return_value = mock_hook

    # Execute function
    from pipeline_monitoring_dag import generate_daily_report
    context = MockAirflowContext().get_context()
    result = generate_daily_report(**context)

    # Verify results
    assert result["total_transactions"] == 1000
    assert result["total_sales"] == 50000.0
```

### Testing Error Scenarios

```python
@patch('subprocess.run')
def test_kafka_health_check_failure(mock_subprocess):
    """Test Kafka health check failure handling"""
    mock_subprocess.side_effect = Exception("Connection failed")

    from cloud_sales_pipeline_dag import check_kafka_health
    context = MockAirflowContext().get_context()

    result = check_kafka_health(**context)

    assert result == "kafka_restart"
    # Verify error was logged via XCom
    context['task_instance'].xcom_push.assert_called_with(
        key="kafka_status", value="error"
    )
```

## Test Data and Fixtures

### Using Test Fixtures

```python
def test_with_mock_context(mock_airflow_context):
    """Test using mock Airflow context"""
    # Context is automatically mocked
    assert mock_airflow_context.ds == "2024-01-01"
    assert mock_airflow_context.task_instance is not None

def test_with_snowflake_mock(mock_snowflake_hook):
    """Test using mock Snowflake hook"""
    # Hook is pre-configured with test data
    result = mock_snowflake_hook.get_first("SELECT 1")
    assert result is not None
```

### Generating Test Data

```python
from tests.airflow.test_utils import TestDataGenerator

def test_with_generated_data():
    """Test using generated test data"""
    # Generate realistic daily metrics
    metrics = TestDataGenerator.generate_daily_metrics(
        date_key=20240101,
        base_transactions=1500
    )

    # Generate quality trends
    trends = TestDataGenerator.generate_quality_trends(
        num_days=7,
        base_date=datetime(2024, 1, 1)
    )

    # Use in tests
    assert len(metrics) == 10
    assert len(trends) == 7
```

## Debugging Tests

### Common Issues and Solutions

1. **Import Errors**:
   ```bash
   # Ensure DAG directory is in Python path
   export PYTHONPATH="${PYTHONPATH}:$(pwd)/airflow/dags"
   ```

2. **Missing Dependencies**:
   ```bash
   # Install all required packages
   pip install -r requirements-dev.txt
   ```

3. **Environment Variable Issues**:
   ```bash
   # Check test environment is loaded
   pytest tests/airflow/ -v -s --capture=no
   ```

4. **Airflow Configuration Issues**:
   ```bash
   # Reset Airflow test configuration
   unset AIRFLOW_HOME
   pytest tests/airflow/ -v
   ```

### Debugging Specific Tests

```bash
# Run single test with full output
pytest tests/airflow/test_cloud_sales_pipeline_dag.py::TestKafkaHealthCheck::test_healthy -v -s

# Drop into debugger on failure
pytest tests/airflow/ --pdb

# Capture stdout/stderr
pytest tests/airflow/ -s --capture=no

# Show local variables on failure
pytest tests/airflow/ -l
```

## Test Coverage

### Measuring Coverage

```bash
# Run tests with coverage
pytest tests/airflow/ --cov=airflow/dags --cov-report=html --cov-report=term

# View coverage report
open htmlcov/index.html

# Coverage thresholds
pytest tests/airflow/ --cov=airflow/dags --cov-fail-under=80
```

### Coverage Goals

- **Overall Coverage**: ≥ 85%
- **Function Coverage**: ≥ 90%
- **Branch Coverage**: ≥ 80%
- **Critical Paths**: 100%

## Continuous Integration

### GitHub Actions Integration

```yaml
# .github/workflows/test.yml
name: Test DAGs
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9

      - name: Install dependencies
        run: |
          pip install -r requirements.txt -r requirements-dev.txt

      - name: Run unit tests
        run: |
          pytest tests/airflow/ -m "unit" --junitxml=unit-results.xml

      - name: Run integration tests
        run: |
          pytest tests/airflow/ -m "integration" --junitxml=integration-results.xml

      - name: Upload test results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: "*-results.xml"
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: pytest-unit
        name: pytest-unit
        entry: pytest tests/airflow/ -m "unit"
        language: system
        pass_filenames: false
        always_run: true
```

## Extending Tests

### Adding New Tests

1. **Create Test File**:
   ```python
   # tests/airflow/test_new_feature.py
   import pytest
   from tests.airflow.test_utils import MockAirflowContext

   class TestNewFeature:
       def test_new_functionality(self):
           # Test implementation
           pass
   ```

2. **Add Test Markers**:
   ```python
   @pytest.mark.unit
   def test_unit_functionality():
       pass

   @pytest.mark.integration
   def test_integration_scenario():
       pass
   ```

3. **Update Documentation**:
   - Add test description to this README
   - Document any new fixtures or utilities
   - Update coverage expectations

### Creating Custom Fixtures

```python
# In conftest.py or test files
@pytest.fixture
def custom_test_data():
    """Fixture for custom test data"""
    return {
        "test_value": 123,
        "test_list": [1, 2, 3]
    }

@pytest.fixture
def mock_external_api():
    """Mock external API calls"""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {"status": "ok"}
        yield mock_get
```

## Additional Resources

- [Airflow Testing Documentation](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag)
- [Pytest Documentation](https://docs.pytest.org/)
- [Python Mock Library](https://docs.python.org/3/library/unittest.mock.html)
- [Testing Best Practices](https://github.com/apache/airflow/blob/main/TESTING.rst)

## Contributing

When adding new tests:

1. Follow existing naming conventions
2. Add appropriate pytest markers
3. Include docstrings explaining test purpose
4. Mock external dependencies
5. Update this documentation
6. Ensure tests pass in CI/CD pipeline

## Test Checklist

- [ ] All DAG files can be imported without errors
- [ ] DAG structure tests pass (tasks, dependencies, configuration)
- [ ] Python function tests cover all branches
- [ ] Error scenarios are tested
- [ ] Integration tests verify cross-DAG workflows
- [ ] Performance tests validate scalability
- [ ] External service mocking is comprehensive
- [ ] Test documentation is up to date
