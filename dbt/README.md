# Sales Data Pipeline - dbt Project

## Overview

This dbt project transforms raw sales data from both streaming (Kafka) and batch (Spark) sources into analytics-ready dimensional models in Snowflake. It implements a modern data warehouse architecture with comprehensive data quality testing and documentation.

## Architecture

### Data Flow
```
Raw Sources (Snowflake)
├── SALES_RAW (Kafka streaming)
├── SALES_BATCH_RAW (Spark batch)
├── PRODUCT_RAW (Master data)
└── STORE_RAW (Master data)
        ↓
Staging Models (Clean & Standardize)
├── stg_sales_raw
├── stg_sales_batch
├── stg_products
└── stg_stores
        ↓
Intermediate Models (Business Logic)
├── int_sales_unified (Combine sources)
├── int_product_scd2 (SCD2 logic)
├── int_store_scd2 (SCD2 logic)
└── int_date_spine (Date generation)
        ↓
Marts (Dimensional Model)
├── dim_date
├── dim_product
├── dim_store
├── dim_customer
├── fact_sales
└── fact_sales_daily
```

### Schema Strategy
- **Raw**: `SALES_DW.RAW` (source tables)
- **Staging**: `SALES_DW.STAGING` (cleaned data)
- **Marts**: `SALES_DW.MARTS` (final dimensional model)

## Key Features

### 1. Data Quality Framework
- **Source Testing**: Comprehensive tests on raw data sources
- **Staging Validation**: Data cleaning and quality scoring
- **Business Rule Testing**: Custom tests for business logic
- **Referential Integrity**: Foreign key relationship testing
- **Singular Tests**: Complex business rule validation

### 2. Slowly Changing Dimensions (SCD2)
- **Product Dimension**: Tracks price changes, category updates
- **Store Dimension**: Tracks location changes, store type updates
- **Version Control**: Complete audit trail of changes
- **Data Integrity**: Automated validation of SCD2 logic

### 3. Unified Sales Model
- **Multi-Source**: Combines Kafka streaming and Spark batch data
- **Data Enrichment**: Business categorizations and derived metrics
- **Quality Scoring**: Enhanced data quality assessment
- **Flexible Schema**: Accommodates different source schemas

### 4. Performance Optimization
- **Daily Aggregates**: Pre-aggregated fact table for fast queries
- **Clustering**: Optimized table clustering for Snowflake
- **Materialization**: Strategic use of tables vs. views
- **Incremental Models**: Efficient processing of large datasets

## Getting Started

### Prerequisites
```bash
# Install dbt and dependencies
pip install -r requirements.txt

# Install dbt packages
dbt deps
```

### Configuration
1. **Copy Profile Template**:
   ```bash
   cp profiles.yml.example ~/.dbt/profiles.yml
   ```

2. **Set Environment Variables**:
   ```bash
   export SNOWFLAKE_ACCOUNT="your-account.region.cloud"
   export SNOWFLAKE_USER="your-username"
   export SNOWFLAKE_PASSWORD="your-password"
   export SNOWFLAKE_DATABASE="SALES_DW"
   export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
   ```

3. **Test Connection**:
   ```bash
   dbt debug
   ```

### Development Workflow

#### Initial Setup
```bash
# Install dependencies
dbt deps

# Create development schema
dbt run --target dev

# Run all tests
dbt test
```

#### Daily Development
```bash
# Run specific model
dbt run --select stg_sales_raw

# Run model and downstream dependencies
dbt run --select stg_sales_raw+

# Run tests for specific model
dbt test --select stg_sales_raw

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

#### Production Deployment
```bash
# Full refresh (development/staging)
dbt run --full-refresh --target staging

# Production deployment
dbt run --target prod
dbt test --target prod
```

## Model Documentation

### Staging Models
- **`stg_sales_raw`**: Cleaned streaming sales data with validation
- **`stg_sales_batch`**: Standardized batch sales data
- **`stg_products`**: Product master data with quality scoring
- **`stg_stores`**: Store master data with geographic enrichment

### Intermediate Models
- **`int_sales_unified`**: Combined streaming + batch sales with business logic
- **`int_product_scd2`**: Product dimension with change tracking
- **`int_store_scd2`**: Store dimension with change tracking
- **`int_date_spine`**: Complete date range for dimension generation

### Dimension Models
- **`dim_date`**: Calendar dimension with business and fiscal attributes
- **`dim_product`**: Product dimension with SCD2 and categorizations
- **`dim_store`**: Store dimension with geographic and operational attributes
- **`dim_customer`**: Customer dimension with RFM segmentation

### Fact Models
- **`fact_sales`**: Transaction-level sales facts with full detail
- **`fact_sales_daily`**: Daily aggregated facts for performance

## Data Quality

### Test Categories

#### Source Tests
- **Uniqueness**: Primary keys and business keys
- **Completeness**: Required fields validation
- **Format**: Data type and range validation
- **Relationships**: Foreign key integrity

#### Business Logic Tests
- **Calculation Accuracy**: Formula validation
- **Data Consistency**: Cross-model validation
- **Temporal Logic**: Date range and SCD2 validation
- **Aggregation Accuracy**: Fact table completeness

#### Custom Tests
- **SCD2 Integrity**: Version control validation
- **Data Completeness**: Source-to-fact coverage
- **Business Rules**: Domain-specific validation

### Quality Scoring
Each record receives a data quality score (0-110) based on:
- Required field completeness (70 points)
- Optional field completeness (20 points)
- Business rule compliance (20 points)

Minimum thresholds:
- **Staging**: 70 points
- **Intermediate**: 75 points
- **Facts**: 80 points

## Performance Optimization

### Snowflake Configuration
```yaml
# Recommended warehouse sizes
Development: X-SMALL
Staging: SMALL
Production: MEDIUM (scale up as needed)

# Clustering strategies
Dimensions: [effective_date, is_current]
Facts: [date_key, store_key]
Daily Facts: [date_key, product_key]
```

### Query Optimization
- Use current views for dimension lookups
- Leverage daily aggregates for trend analysis
- Filter on clustered columns when possible
- Use appropriate materialization strategies

## Monitoring & Alerts

### dbt Cloud Integration
- **Scheduled Runs**: Daily production refresh
- **Test Failures**: Immediate alerts
- **Documentation**: Auto-generated and updated

### Custom Monitoring
```sql
-- Data freshness check
select max(ingestion_timestamp) as latest_data
from {{ ref('stg_sales_raw') }};

-- Quality score monitoring
select
    avg(data_quality_score) as avg_quality,
    min(data_quality_score) as min_quality
from {{ ref('fact_sales') }}
where date_key >= to_number(to_char(current_date(), 'YYYYMMDD'));
```

## Troubleshooting

### Common Issues

#### 1. Connection Errors
```bash
# Verify credentials
dbt debug

# Test specific connection
dbt run --select stg_sales_raw --target dev
```

#### 2. Test Failures
```bash
# Run specific test
dbt test --select test_name

# Run tests with verbose output
dbt test --store-failures
```

#### 3. Performance Issues
```bash
# Check query performance
select query_text, execution_time
from snowflake.account_usage.query_history
where query_text like '%dbt%'
order by execution_time desc;
```

#### 4. Data Quality Issues
```bash
# Check quality scores
dbt run --select int_sales_unified
dbt test --select int_sales_unified

# Review failed records
select * from <schema>_tests.failed_records_<test_name>;
```

## Contributing

### Code Standards
- Follow dbt style guide
- Use descriptive model names
- Add comprehensive documentation
- Include appropriate tests
- Use consistent formatting

### Development Process
1. Create feature branch
2. Develop and test locally
3. Run full test suite
4. Submit pull request
5. Deploy to staging
6. Validate in production

## Support

For issues or questions:
- Check dbt logs: `logs/dbt.log`
- Review test results: `target/run_results.json`
- Consult documentation: `dbt docs serve`
- Contact data engineering team
