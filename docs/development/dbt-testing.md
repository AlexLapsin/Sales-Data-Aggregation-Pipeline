# DBT Testing Comprehensive Guide

This document provides a complete guide to the comprehensive testing strategy implemented for the Sales Data Aggregation Pipeline dbt project.

## Overview

Our testing framework ensures data quality, transformation correctness, and pipeline reliability through multiple layers of validation:

- **Schema Tests**: Column-level and table-level validations
- **Singular Tests**: Custom business logic validations
- **Generic Tests**: Reusable test patterns
- **Performance Tests**: Query optimization and resource usage validation
- **Documentation Tests**: Completeness and quality checks
- **Project Health Checks**: Structure and best practices validation

## Test Categories

### 1. Schema Tests (Generic)

Located in `models/*/_.yml` files, these tests validate:

- **Data Types & Constraints**: `not_null`, `unique`, `accepted_values`
- **Referential Integrity**: `relationships` between facts and dimensions
- **Value Ranges**: Custom `acceptable_range` tests
- **Cardinality**: `reasonable_cardinality` tests for dimension validation
- **String Constraints**: `string_length` validations

**Example Usage:**
```yaml
columns:
  - name: sales_amount
    tests:
      - not_null
      - acceptable_range:
          min_value: 0.01
          max_value: 100000.00
```

### 2. Singular Tests

Located in `tests/singular/`, these validate complex business rules:

- **test_source_data_quality.sql**: Comprehensive source data validation
- **test_dimensional_model_integrity.sql**: Star schema compliance checks
- **test_business_logic_validation.sql**: End-to-end business rule validation
- **test_scd2_integrity.sql**: Slowly Changing Dimension validation
- **test_fact_sales_completeness.sql**: Data pipeline completeness checks
- **test_daily_aggregates_accuracy.sql**: Aggregation accuracy validation

**Example Test:**
```sql
-- Ensure profit margins are reasonable
select *
from {{ ref('fact_sales') }}
where profit_amount / net_sales_amount > 0.95  -- >95% profit margin (suspicious)
   or profit_amount / net_sales_amount < -2.0  -- Loss >200% of sales
```

### 3. Custom Generic Tests

Located in `macros/generic_tests.sql`, providing reusable test patterns:

- **acceptable_range**: Enhanced range validation with inclusive/exclusive bounds
- **string_length**: String length constraints
- **sequential_dates**: Date gap detection
- **reasonable_cardinality**: Cardinality validation
- **mutually_exclusive_ranges**: Date range validation
- **enhanced_relationships**: Foreign key validation with detailed error reporting
- **scd2_integrity**: Complete SCD2 implementation validation

### 4. Data Quality Macros

Located in `macros/data_quality.sql`, providing specialized validations:

- **test_null_rate**: Null rate thresholds
- **test_data_distribution**: Statistical distribution checks
- **test_referential_integrity**: Cross-table referential checks
- **test_business_rule_compliance**: Business rule validation framework
- **test_cross_model_consistency**: Inter-model consistency checks

### 5. Performance Tests

Located in `macros/performance_tests.sql`:

- **test_model_performance**: Execution time and resource usage
- **test_join_efficiency**: Cartesian product detection
- **test_clustering_effectiveness**: Index/clustering validation
- **test_incremental_efficiency**: Incremental model optimization
- **test_model_freshness**: Data staleness detection

## Testing Layers

### Bronze Layer (Raw Data)
- Source freshness validation
- Data format and structure checks
- Business key integrity
- Value range validations

### Silver Layer (Staging Models)
- Data cleaning validation
- Transformation accuracy
- Quality score calculations
- Standardization checks

### Gold Layer (Marts)
- Dimensional model integrity
- Referential integrity across star schema
- Business rule compliance
- Aggregation accuracy
- SCD2 implementation validation

## Running Tests

### Local Development

```bash
# Run all tests
dbt test

# Run specific test categories
dbt test --select test_type:schema
dbt test --select test_type:singular

# Run tests for specific models
dbt test --select stg_sales_raw
dbt test --select +fact_sales  # Include upstream dependencies

# Run enhanced tests
dbt test --select test_source_data_quality
dbt test --select test_business_logic_validation
```

### Comprehensive Test Suite

```bash
# Run complete test orchestration
python scripts/run_comprehensive_tests.py

# Run with specific categories
python scripts/run_comprehensive_tests.py --categories compilation schema_tests singular_tests

# Run against specific target
python scripts/run_comprehensive_tests.py --target prod --fail-fast
```

### Project Health Check

```bash
# Check project structure and best practices
python scripts/check_project_health.py

# Generate detailed report
python scripts/check_project_health.py --output-file health_report.txt
```

### CI/CD Integration

Tests run automatically via GitHub Actions:
- **Pull Requests**: Schema and singular tests
- **Main Branch**: Full comprehensive test suite
- **Manual Trigger**: Configurable test execution

## Test Configuration

### Test Severity Levels

```yaml
tests:
  - not_null:
      config:
        severity: error  # Fails build
  - acceptable_values:
      values: [...]
      config:
        severity: warn   # Logs warning but continues
```

### Test Storage

```yaml
# dbt_project.yml
tests:
  sales_data_pipeline:
    +store_failures: true      # Store test failures for analysis
    +schema: staging_tests     # Dedicated schema for test results
```

### Freshness Configuration

```yaml
# _sources.yml
freshness:
  warn_after: {count: 2, period: hour}
  error_after: {count: 6, period: hour}
  filter: partition_date = current_date()
```

## Test Development Guidelines

### Writing Effective Tests

1. **Use Descriptive Names**: Test names should clearly indicate what is being validated
2. **Add Context**: Include comments explaining business logic
3. **Consider Performance**: Avoid expensive tests in CI pipelines
4. **Test Edge Cases**: Include boundary conditions and null handling
5. **Use Appropriate Severity**: Reserve 'error' for critical failures

### Test Organization

```
tests/
├── singular/
│   ├── business_logic/
│   ├── data_quality/
│   └── performance/
└── pytest/
    ├── test_dbt_project.py
    └── test_macro_functionality.py
```

### Best Practices

- **Layer-Appropriate Testing**: More rigorous testing at higher layers
- **Business Rule Focus**: Test business logic, not just technical constraints
- **Performance Awareness**: Balance thoroughness with execution time
- **Documentation**: Document complex test logic and thresholds
- **Regular Review**: Periodically review and update test thresholds

## Troubleshooting

### Common Test Failures

1. **Source Freshness Failures**
   - Check data pipeline status
   - Review ETL job logs
   - Verify source system availability

2. **Referential Integrity Failures**
   - Check dimension loading order
   - Verify business key consistency
   - Review SCD2 implementation

3. **Business Rule Violations**
   - Validate source data quality
   - Check transformation logic
   - Review business rule definitions

### Debugging Tests

```bash
# Run test with debug output
dbt test --select test_name --store-failures

# Query failed test results
select * from {{ ref('test_name') }}

# Analyze test execution
dbt run-operation log_test_results
```

## Monitoring and Alerting

### Test Result Monitoring

- CI/CD pipeline notifications
- Test failure trend analysis
- Data quality score tracking
- Performance regression detection

### Alerting Thresholds

- **Critical**: Schema test failures, referential integrity issues
- **Warning**: Data quality score degradation, freshness delays
- **Info**: Performance changes, documentation gaps

## Continuous Improvement

### Test Metrics

Track and analyze:
- Test execution times
- Failure rates by category
- Coverage metrics
- False positive rates

### Regular Reviews

- Monthly test effectiveness review
- Quarterly threshold adjustments
- Annual test strategy assessment
- Continuous business rule updates

## Tools and Dependencies

### Required Python Packages

```txt
dbt-core>=1.6.0
dbt-snowflake>=1.6.0
pytest>=7.0.0
pyyaml>=6.0
pandas>=1.5.0
```

### dbt Packages

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations
    version: 0.10.0
  - package: elementary-data/elementary
    version: 0.13.2
```

For additional support or questions about the testing framework, please refer to the project documentation or contact the data engineering team.
