# Kafka Producer Test Suite

This directory contains comprehensive tests for the Kafka Producer component (`streaming/kafka_producer.py`). The test suite validates data generation quality, Kafka connectivity, error handling, performance, and schema compliance.

## Test Organization

### Test Categories

1. **Unit Tests** (`TestSalesDataGenerator`, `TestKafkaSalesProducerUnit`)
   - Fast execution, no external dependencies
   - Uses mocking for Kafka components
   - Tests data generation logic and producer configuration

2. **Integration Tests** (`TestKafkaSalesProducerIntegration`)
   - Requires running Kafka instance
   - Tests real Kafka connectivity and message delivery
   - Validates end-to-end message flow

3. **Performance Tests** (`TestPerformanceAndLoad`)
   - Load testing and throughput validation
   - Memory usage monitoring
   - Concurrent producer testing

4. **Schema Tests** (`TestMessageSchemaValidation`)
   - JSON schema validation
   - Message format compliance
   - Serialization/deserialization testing

5. **Error Handling Tests** (`TestErrorHandlingAndRetries`)
   - Connection failure scenarios
   - Retry logic validation
   - Error logging verification

6. **Configuration Tests** (`TestConfigurationScenarios`)
   - Docker Kafka setup testing
   - AWS MSK configuration testing
   - Environment variable handling

## Quick Start

### Prerequisites

```bash
# Install test dependencies
pip install -r requirements-test.txt

# For integration tests, ensure Kafka is running:
# Option 1: Docker Kafka
docker-compose up kafka

# Option 2: Local Kafka installation
# Start Zookeeper and Kafka according to your setup
```

### Running Tests

#### Quick Development Tests (Unit + Schema only)
```bash
python tests/run_kafka_producer_tests.py --mode quick
```

#### Unit Tests Only (No Kafka Required)
```bash
python tests/run_kafka_producer_tests.py --mode unit
```

#### Integration Tests (Requires Kafka)
```bash
python tests/run_kafka_producer_tests.py --mode integration
```

#### Performance Tests
```bash
python tests/run_kafka_producer_tests.py --mode performance
```

#### Complete Test Suite
```bash
python tests/run_kafka_producer_tests.py --mode all
```

#### Generate Comprehensive Report
```bash
python tests/run_kafka_producer_tests.py --mode report
```

### Direct pytest Execution

You can also run tests directly with pytest:

```bash
# Run all tests
pytest tests/test_kafka_producer.py -v

# Run specific test categories
pytest tests/test_kafka_producer.py -m unit -v
pytest tests/test_kafka_producer.py -m integration -v
pytest tests/test_kafka_producer.py -m performance -v

# Run with coverage
pytest tests/test_kafka_producer.py --cov=streaming.kafka_producer --cov-report=html

# Run specific test class
pytest tests/test_kafka_producer.py::TestSalesDataGenerator -v

# Run specific test method
pytest tests/test_kafka_producer.py::TestSalesDataGenerator::test_event_data_realism -v
```

## Test Configuration

### Environment Variables

```bash
# Kafka connection
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="sales_events"

# For AWS MSK
export KAFKA_BOOTSTRAP_SERVERS="b-1.msk-cluster.kafka.us-west-2.amazonaws.com:9092,b-2.msk-cluster.kafka.us-west-2.amazonaws.com:9092"
export AWS_REGION="us-west-2"
```

### Configuration Files

Test configurations for different environments are provided in `kafka_test_configs/`:

- `docker_kafka_config.json` - Local Docker Kafka setup
- `aws_msk_config.json` - AWS MSK configuration

## Test Details

### Data Generation Tests (`TestSalesDataGenerator`)

**Purpose**: Validate that generated sales data is realistic and well-formed

**Key Tests**:
- `test_generator_initialization`: Verifies proper data structure setup
- `test_event_field_types`: Ensures correct data types for all fields
- `test_event_data_realism`: Validates realistic data distributions
- `test_price_ranges_by_category`: Verifies prices fall within expected ranges
- `test_discount_application`: Tests discount calculation accuracy
- `test_unique_identifiers`: Ensures ID uniqueness and proper formatting
- `test_generation_performance`: Benchmarks data generation speed

**Expected Performance**: >1000 events/second generation rate

### Producer Unit Tests (`TestKafkaSalesProducerUnit`)

**Purpose**: Test producer functionality with mocked Kafka dependencies

**Key Tests**:
- `test_producer_initialization`: Validates configuration setup
- `test_producer_configuration_parameters`: Verifies Kafka producer config
- `test_produce_single_event_success`: Tests successful message production
- `test_produce_event_kafka_error`: Tests error handling
- `test_produce_continuous_with_count`: Tests batch production
- `test_produce_continuous_keyboard_interrupt`: Tests graceful shutdown

### Integration Tests (`TestKafkaSalesProducerIntegration`)

**Purpose**: Test with real Kafka instances

**Prerequisites**: Running Kafka cluster accessible at `KAFKA_BOOTSTRAP_SERVERS`

**Key Tests**:
- `test_real_kafka_connection`: Validates actual Kafka connectivity
- `test_message_delivery_and_consumption`: End-to-end message flow
- `test_partitioning_with_keys`: Verifies message partitioning
- `test_production_throughput`: Measures real-world performance

**Expected Performance**: >50 events/second with real Kafka

### Schema Validation Tests (`TestMessageSchemaValidation`)

**Purpose**: Ensure message format compliance and data quality

**Key Tests**:
- `test_generated_events_match_schema`: JSON schema validation
- `test_serialization_roundtrip`: JSON serialization testing
- `test_message_size_constraints`: Validates reasonable message sizes

**Schema Requirements**:
- All required fields present
- Correct data types for each field
- Valid format for timestamps and IDs
- Message size under 1KB

### Error Handling Tests (`TestErrorHandlingAndRetries`)

**Purpose**: Validate error scenarios and retry behavior

**Key Tests**:
- `test_connection_failure_handling`: Tests invalid broker handling
- `test_retry_configuration`: Verifies retry settings
- `test_error_logging`: Ensures proper error logging

### Performance Tests (`TestPerformanceAndLoad`)

**Purpose**: Validate performance under various load conditions

**Key Tests**:
- `test_memory_usage_during_generation`: Memory usage monitoring
- `test_concurrent_producers`: Multiple producer testing
- `test_batch_production_performance`: Batch processing efficiency

**Performance Targets**:
- Memory usage: <10MB for 1000 events
- Concurrent producers: 3+ simultaneous producers
- Batch consistency: <50% throughput deviation across batch sizes

## Test Markers

Use pytest markers to run specific test categories:

```bash
# Available markers
-m unit              # Unit tests only
-m integration       # Integration tests only
-m performance       # Performance tests only
-m schema           # Schema validation tests
-m error_handling   # Error handling tests
-m config           # Configuration tests

# Combine markers
-m "unit or schema"         # Unit and schema tests
-m "not performance"        # All except performance tests
-m "integration and not performance"  # Integration tests excluding performance
```

## Continuous Integration

### GitHub Actions Integration

```yaml
# Example CI configuration
- name: Run Kafka Producer Tests
  run: |
    # Install dependencies
    pip install -r requirements-test.txt

    # Run unit tests (no Kafka required)
    python tests/run_kafka_producer_tests.py --mode unit

    # Run schema validation
    python tests/run_kafka_producer_tests.py --mode schema

    # Upload coverage reports
    codecov --file coverage.xml
```

### Local Development Workflow

```bash
# 1. Quick validation during development
python tests/run_kafka_producer_tests.py --mode quick

# 2. Before committing changes
python tests/run_kafka_producer_tests.py --mode unit
python tests/run_kafka_producer_tests.py --mode schema

# 3. Before deployment (with Kafka running)
python tests/run_kafka_producer_tests.py --mode all
```

## Troubleshooting

### Common Issues

1. **"Kafka not available"**
   - Ensure Kafka is running: `docker-compose up kafka`
   - Verify `KAFKA_BOOTSTRAP_SERVERS` environment variable
   - Check network connectivity to Kafka brokers

2. **Import errors**
   - Install test dependencies: `pip install -r requirements-test.txt`
   - Ensure `streaming/` directory is in Python path

3. **Performance tests failing**
   - Reduce system load during testing
   - Adjust performance thresholds in test code if needed
   - Check available memory and CPU resources

4. **Schema validation failures**
   - Verify generated data structure hasn't changed
   - Update schema definition if data model evolved
   - Check for new required fields

### Test Coverage

Current test coverage targets:
- **Overall**: >90% line coverage
- **Data generation**: 100% coverage
- **Producer logic**: >95% coverage
- **Error handling**: >85% coverage

### Performance Benchmarks

Expected performance baselines:
- **Data generation**: >1000 events/second
- **Unit test execution**: <30 seconds
- **Integration tests**: <2 minutes (with Kafka)
- **Memory usage**: <10MB for 1000 events

## Contributing

When adding new tests:

1. Follow existing naming conventions
2. Add appropriate pytest markers
3. Include docstrings describing test purpose
4. Update this README if adding new test categories
5. Ensure tests are deterministic and repeatable
6. Add performance assertions for performance-critical code

## Files

- `test_kafka_producer.py` - Main test file
- `run_kafka_producer_tests.py` - Test runner script
- `pytest.ini` - Pytest configuration
- `kafka_test_configs/` - Environment-specific configurations
- `README_KAFKA_PRODUCER_TESTS.md` - This documentation
