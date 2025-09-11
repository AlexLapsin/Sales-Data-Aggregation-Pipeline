# End-to-End Integration Testing Framework

This comprehensive testing framework validates the entire sales data aggregation pipeline from raw data ingestion through final analytics tables.

## Overview

The testing framework provides:
- **Complete pipeline validation** from CSV inputs to final warehouse
- **Performance monitoring** and benchmarking across all components
- **Error scenario testing** with fault injection and recovery validation
- **Data quality validation** at each pipeline stage
- **Infrastructure management** for test environments
- **Automated cleanup** and teardown procedures

## Architecture

### Data Flow Testing
```
Raw CSV Files → Kafka Streaming → Spark ETL → dbt Transformations → Snowflake/PostgreSQL
      ↓               ↓              ↓               ↓                    ↓
   Validator      Validator      Validator      Validator           Validator
```

### Components

#### Core Framework
- **`e2e_config.py`** - Centralized configuration management
- **`test_e2e_pipeline.py`** - Main test suite and orchestration
- **`data_validators.py`** - Data quality validation at each stage
- **`test_data_generator.py`** - Realistic test data generation
- **`infrastructure_manager.py`** - Test environment setup/teardown
- **`performance_monitor.py`** - Performance tracking and analysis
- **`error_scenario_tests.py`** - Fault injection and recovery testing

#### Test Scenarios
1. **Happy Path** - Clean data, optimal conditions
2. **Data Quality Issues** - Intentional data problems
3. **High Volume** - Scalability testing
4. **Edge Cases** - Boundary conditions
5. **Performance Benchmark** - Large dataset processing
6. **Error Recovery** - Fault tolerance validation

## Quick Start

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt
pip install pytest pytest-asyncio docker boto3 psycopg2-binary kafka-python

# Ensure Docker is running
docker --version
```

### Basic Usage

#### Run Happy Path Test
```bash
# Run the main happy path test
python tests/integration/test_e2e_pipeline.py --test happy_path --record-count 1000

# Or use pytest
pytest tests/integration/test_e2e_pipeline.py::E2EPipelineTestSuite::test_happy_path_pipeline -v
```

#### Run Complete Test Suite
```bash
# Run all end-to-end tests
python tests/integration/test_e2e_pipeline.py --test all

# Run with custom configuration
python tests/integration/test_e2e_pipeline.py --test all --log-level DEBUG
```

#### Run Error Scenario Tests
```bash
# Run all error scenarios
python tests/integration/error_scenario_tests.py --all

# Run specific error scenario
python tests/integration/error_scenario_tests.py --scenario kafka_service_stop
```

## Configuration

### Environment Variables
```bash
# Test data sizes
export E2E_SMALL_DATASET_SIZE=100
export E2E_MEDIUM_DATASET_SIZE=1000
export E2E_LARGE_DATASET_SIZE=10000

# Infrastructure ports
export E2E_KAFKA_PORT=29092
export E2E_POSTGRES_PORT=15432

# Test behavior
export E2E_LOG_LEVEL=INFO
```

### Custom Configuration File
```yaml
# tests/integration/config/custom.yml
test_data:
  small_dataset_size: 500
  medium_dataset_size: 5000
  large_dataset_size: 50000

infrastructure:
  kafka_port: 29092
  postgres_port: 15432
  cleanup_containers_on_success: true

pipeline_stages:
  min_data_quality_score: 0.85
  max_duplicate_percentage: 0.02
```

## Test Data Generation

### Built-in Scenarios
```python
from tests.integration.test_data_generator import TestDataGenerator

generator = TestDataGenerator()

# Generate different scenarios
happy_path_data = generator.generate_test_scenario_data("happy_path", record_count=1000)
quality_issues_data = generator.generate_test_scenario_data("data_quality_issues", record_count=1000)
high_volume_data = generator.generate_test_scenario_data("high_volume", record_count=50000)
```

### Custom Data Generation
```python
from tests.integration.test_data_generator import DataGenerationConfig

# Custom configuration
config = DataGenerationConfig(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    num_customers=5000,
    error_rate=0.05  # 5% error rate
)

generator = TestDataGenerator(config)
test_data = generator.generate_csv_data(10000)
```

## Performance Testing

### Monitoring Components
```python
from tests.integration.performance_monitor import PerformanceMonitor

monitor = PerformanceMonitor()

# Monitor specific component
report = await monitor.monitor_kafka_streaming(
    bootstrap_servers="localhost:29092",
    topic="sales_events",
    duration_seconds=60
)

# Benchmark throughput
benchmark = await monitor.run_throughput_benchmark(
    component_name="spark_etl",
    workload_sizes=[1000, 5000, 10000, 50000]
)
```

### Performance Assertions
```python
# In test cases
assert kafka_throughput >= 100  # messages/second
assert spark_throughput >= 1000  # records/second
assert overall_duration <= 300  # seconds
```

## Error Scenario Testing

### Predefined Error Scenarios
- **Service Failures**: Kafka/PostgreSQL/Spark crashes
- **Data Corruption**: Invalid CSV/JSON, schema mismatches
- **Network Issues**: Latency, timeouts, packet loss
- **Resource Exhaustion**: Memory/disk/CPU limits

### Custom Error Injection
```python
from tests.integration.error_scenario_tests import ErrorInjector

injector = ErrorInjector(infrastructure_manager)

# Inject service failure
error_id = await injector.inject_service_failure("kafka", "stop")

# Monitor recovery
await asyncio.sleep(30)

# Recover from error
await injector.recover_from_error(error_id)
```

## Data Validation

### Validation at Each Stage
```python
from tests.integration.data_validators import PipelineValidator

validator = PipelineValidator()

# Validate complete pipeline run
validation_data = {
    "raw_data": csv_dataframe,
    "kafka_streaming": {"messages": kafka_messages},
    "spark_etl": {"input_data": input_df, "output_data": output_df},
    "data_warehouse": {"connection": db_connection}
}

summary = validator.validate_pipeline_run("test_run_123", validation_data)
```

### Quality Thresholds
- **Data Quality Score**: ≥ 80%
- **Null Rate**: ≤ 5%
- **Duplicate Rate**: ≤ 1%
- **Schema Compliance**: 100%

## Infrastructure Management

### Automatic Setup
```python
from tests.integration.infrastructure_manager import InfrastructureManager

infrastructure = InfrastructureManager(config)

# Start all services
await infrastructure.start_services()
await infrastructure.wait_for_readiness()

# Setup databases and topics
await infrastructure.setup_databases()
await infrastructure.setup_kafka_topics()

# Cleanup when done
await infrastructure.cleanup()
```

### Manual Docker Compose
```bash
# Use generated docker-compose file
docker-compose -f tests/integration/docker-compose.test.yml up -d

# Check service status
docker-compose -f tests/integration/docker-compose.test.yml ps

# Cleanup
docker-compose -f tests/integration/docker-compose.test.yml down -v
```

## Test Artifacts and Reporting

### Generated Artifacts
```
tests/integration/artifacts/
├── test_run_20241201_143022_a1b2c3d4/
│   ├── input_data.csv
│   ├── kafka_messages.json
│   ├── expected_output.csv
│   ├── validation_report.json
│   ├── performance_metrics.json
│   └── metadata.json
```

### Performance Reports
```json
{
  "report_metadata": {
    "generated_at": "2024-12-01T14:30:22",
    "total_monitoring_duration": 180
  },
  "component_reports": [...],
  "performance_analysis": {
    "bottleneck_analysis": {
      "bottlenecks_found": 1,
      "bottlenecks": [
        {
          "component": "spark_processing",
          "type": "memory",
          "severity": "medium",
          "value": 85.2
        }
      ]
    }
  }
}
```

### Validation Reports
```json
{
  "pipeline_run_id": "test_run_123",
  "overall_passed": true,
  "overall_score": 0.92,
  "stage_results": {
    "raw_data": [
      {
        "check_name": "schema_validation",
        "passed": true,
        "score": 1.0,
        "details": {...}
      }
    ]
  }
}
```

## CI/CD Integration

### GitHub Actions Example
```yaml
name: E2E Pipeline Tests

on: [push, pull_request]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v3
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r tests/requirements.txt

      - name: Run E2E Tests
        run: |
          python tests/integration/test_e2e_pipeline.py --test all --log-level INFO

      - name: Run Error Scenario Tests
        run: |
          python tests/integration/error_scenario_tests.py --all

      - name: Upload Test Reports
        uses: actions/upload-artifact@v3
        with:
          name: test-reports
          path: tests/integration/reports/
```

## Debugging and Troubleshooting

### Common Issues

#### Service Startup Failures
```bash
# Check Docker logs
docker logs test-kafka
docker logs test-postgres

# Verify port availability
netstat -tulpn | grep :29092
netstat -tulpn | grep :15432
```

#### Test Data Issues
```bash
# Verify test data generation
python -c "
from tests.integration.test_data_generator import TestDataGenerator
gen = TestDataGenerator()
data = gen.generate_csv_data(100)
print(f'Generated {len(data)} records')
print(data.head())
"
```

#### Validation Failures
```bash
# Run validation manually
python -c "
from tests.integration.data_validators import RawDataValidator
import pandas as pd
validator = RawDataValidator()
df = pd.read_csv('test_data.csv')
results = validator.validate(df)
for result in results:
    print(f'{result.check_name}: {result.passed} (score: {result.score})')
"
```

### Debug Mode
```bash
# Enable debug logging
export E2E_LOG_LEVEL=DEBUG

# Preserve containers on failure
export E2E_CLEANUP_ON_FAILURE=false

# Keep test data
export E2E_PRESERVE_TEST_DATA=true
```

## Advanced Usage

### Custom Test Scenarios
```python
class CustomTestSuite(E2EPipelineTestSuite):
    async def test_custom_scenario(self):
        # Custom test implementation
        result = await self.run_complete_pipeline_test(
            scenario_name="custom",
            record_count=5000,
            custom_param="value"
        )

        # Custom assertions
        assert result.success
        assert result.performance_metrics["custom_metric"] > threshold
```

### Integration with External Systems
```python
# Test with real cloud services
config_override = {
    "test_environment": {
        "test_env_vars": {
            "SNOWFLAKE_ACCOUNT": "real_account",
            "AWS_S3_BUCKET": "real_bucket"
        }
    }
}

suite = E2EPipelineTestSuite(config_override)
```

### Continuous Performance Monitoring
```python
# Schedule regular performance tests
import schedule

def run_performance_benchmark():
    asyncio.run(suite.test_performance_benchmark())

schedule.every().day.at("02:00").do(run_performance_benchmark)
```

## Best Practices

### Test Design
1. **Isolation**: Each test should be independent
2. **Determinism**: Tests should produce consistent results
3. **Speed**: Balance thoroughness with execution time
4. **Clarity**: Clear test names and error messages

### Data Management
1. **Realistic Data**: Use production-like data patterns
2. **Data Privacy**: No real customer data in tests
3. **Versioning**: Track test data versions
4. **Cleanup**: Always clean up test data

### Infrastructure
1. **Resource Limits**: Set appropriate container limits
2. **Monitoring**: Monitor test infrastructure health
3. **Isolation**: Use separate test environments
4. **Documentation**: Document infrastructure requirements

### Error Handling
1. **Graceful Degradation**: Test should handle partial failures
2. **Recovery**: Test recovery mechanisms
3. **Logging**: Comprehensive error logging
4. **Notification**: Alert on test failures

## Extending the Framework

### Adding New Validators
```python
class CustomValidator(BaseDataValidator):
    def __init__(self):
        super().__init__("custom_stage")

    def validate(self, data, **kwargs):
        # Custom validation logic
        return [self._create_result(...)]
```

### Adding New Error Scenarios
```python
custom_scenario = ErrorScenario(
    name="custom_failure",
    description="Custom failure scenario",
    error_type="custom",
    component="custom_component",
    failure_point="processing",
    expected_behavior="retry",
    recovery_mechanism="automatic"
)
```

### Custom Performance Metrics
```python
class CustomPerformanceMonitor(PerformanceMonitor):
    async def monitor_custom_component(self, duration_seconds):
        # Custom monitoring logic
        return ComponentPerformanceReport(...)
```

## Support and Contributions

### Getting Help
- Check logs in `tests/integration/logs/`
- Review generated test reports
- Use debug mode for detailed output

### Contributing
1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit pull request

### Reporting Issues
Include:
- Test configuration
- Error logs
- System information
- Steps to reproduce

---

For more detailed information, see the individual module documentation and inline code comments.
