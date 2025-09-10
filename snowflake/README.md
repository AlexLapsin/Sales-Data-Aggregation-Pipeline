# Snowflake Data Warehouse Setup

This directory contains the DDL scripts and configuration files for setting up the Snowflake data warehouse component of the sales data pipeline.

## Overview

The Snowflake implementation provides a cloud-native data warehouse with the following architecture:

- **RAW Schema**: Direct ingestion from Kafka streams and batch sources
- **STAGING Schema**: Data cleansing and transformation workspace
- **MARTS Schema**: Star schema dimensional model for analytics
- **UTILS Schema**: Pipeline metadata and utility functions

## Database Schema

### Star Schema Design

**Dimension Tables:**
- `DIM_DATE`: Calendar dimension with business date attributes
- `DIM_PRODUCT`: Product master with slowly changing dimensions
- `DIM_STORE`: Store locations and operational attributes
- `DIM_CUSTOMER`: Customer demographics and segmentation
- `DIM_TIME`: Time-of-day dimension for intraday analysis

**Fact Tables:**
- `FACT_SALES`: Transaction-level sales events (grain: one row per sale)
- `FACT_SALES_DAILY`: Daily aggregated sales (grain: one row per product/store/day)
- `FACT_INVENTORY`: Daily inventory positions and movements
- `FACT_CUSTOMER_BEHAVIOR`: Customer engagement and behavioral metrics

## Setup Instructions

### Prerequisites

1. **Snowflake Account**: Active Snowflake account with appropriate privileges
2. **Python Environment**: Python 3.8+ with required packages
3. **Environment Variables**: Snowflake credentials configured

### Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**
   ```bash
   export SNOWFLAKE_ACCOUNT="your_account.region.cloud"
   export SNOWFLAKE_USER="your_username"
   export SNOWFLAKE_PASSWORD="your_password"
   export SNOWFLAKE_ROLE="SYSADMIN"
   export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
   ```

3. **Update configuration file:**
   - Edit `snowflake_connection_config.json`
   - Replace placeholder values with your Snowflake details
   - Configure environment-specific settings

### Database Setup

#### Automated Setup

```bash
# Setup development environment
python setup_snowflake.py --environment development --verbose

# Setup staging environment
python setup_snowflake.py --environment staging --config custom_config.json

# Setup production environment
python setup_snowflake.py --environment production
```

#### Manual Setup

Execute SQL files in order:

```sql
-- 1. Create database and warehouses
SOURCE 01_database_setup.sql

-- 2. Create raw data tables
SOURCE 02_raw_tables.sql

-- 3. Create dimensional tables
SOURCE 03_dimensional_tables.sql

-- 4. Create fact tables
SOURCE 04_fact_tables.sql
```

### Verification

After setup, verify the installation:

```sql
-- Check databases
SHOW DATABASES LIKE 'SALES_DW%';

-- Check schemas
USE DATABASE SALES_DW;
SHOW SCHEMAS;

-- Check tables in each schema
SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA MARTS;

-- Verify table structures
DESCRIBE TABLE RAW.SALES_RAW;
DESCRIBE TABLE MARTS.FACT_SALES;
```

## Configuration Reference

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `account` | Snowflake account identifier | Required |
| `user` | Username for authentication | Required |
| `password` | User password (use env var) | Required |
| `role` | Snowflake role to assume | SYSADMIN |
| `warehouse` | Default warehouse | ETL_WH |
| `database` | Default database | SALES_DW |
| `schema` | Default schema | RAW |

### Warehouse Configuration

**ETL_WH (Extract, Transform, Load)**
- Size: X-SMALL
- Auto-suspend: 5 minutes
- Auto-resume: Enabled
- Usage: Data loading and transformation operations

**COMPUTE_WH (Analytics and Reporting)**
- Size: X-SMALL
- Auto-suspend: 5 minutes
- Auto-resume: Enabled
- Usage: Analytics queries and dbt transformations

### Performance Optimization

**Clustering Keys:**
- `FACT_SALES`: Clustered by `(DATE_KEY, STORE_KEY)`
- `FACT_SALES_DAILY`: Clustered by `(DATE_KEY, STORE_KEY)`
- `DIM_DATE`: Clustered by `(DATE_VALUE, YEAR_NUMBER)`
- `DIM_PRODUCT`: Clustered by `(CATEGORY, IS_CURRENT)`

**Indexing Strategy:**
- Primary keys on all dimension tables
- Foreign key constraints on fact tables
- Composite indexes on frequently queried columns

## Data Loading Patterns

### Streaming Data (Kafka)
```sql
-- Raw streaming events
INSERT INTO RAW.SALES_RAW
SELECT * FROM kafka_stream_table;
```

### Batch Data (ETL)
```sql
-- Batch file ingestion
COPY INTO RAW.SALES_BATCH_RAW
FROM @external_stage/batch_files/
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',');
```

### Dimensional Updates
```sql
-- Slowly changing dimension updates
MERGE INTO MARTS.DIM_PRODUCT AS target
USING staging_product_updates AS source
ON target.PRODUCT_ID = source.PRODUCT_ID
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

## Monitoring and Maintenance

### Query Monitoring
```sql
-- Monitor query performance
SELECT * FROM INFORMATION_SCHEMA.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%FACT_SALES%'
ORDER BY START_TIME DESC;

-- Check warehouse usage
SELECT * FROM INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME IN ('ETL_WH', 'COMPUTE_WH');
```

### Data Quality Checks
```sql
-- Row count validation
SELECT
    'SALES_RAW' as table_name,
    COUNT(*) as row_count,
    MIN(SALE_TIMESTAMP) as min_date,
    MAX(SALE_TIMESTAMP) as max_date
FROM RAW.SALES_RAW;

-- Data freshness check
SELECT DATEDIFF('minute', MAX(INGESTION_TIMESTAMP), CURRENT_TIMESTAMP()) as minutes_since_last_load
FROM RAW.SALES_RAW;
```

### Maintenance Tasks
```sql
-- Update table statistics
ALTER TABLE MARTS.FACT_SALES REBUILD;

-- Optimize clustering
ALTER TABLE MARTS.FACT_SALES RECLUSTER;

-- Clean up old data
DELETE FROM RAW.SALES_RAW
WHERE PARTITION_DATE < DATEADD('day', -90, CURRENT_DATE());
```

## Integration Points

### Kafka Connect
- Sink connector configuration in `../streaming/snowflake_connector.json`
- Automatic schema detection and table creation
- Real-time data streaming with micro-batch loading

### dbt Transformations
- dbt models reference these base tables
- Transformation logic in `../dbt/models/`
- Incremental models for efficient processing

### Spark ETL Jobs
- Batch processing jobs in `../spark/`
- Direct writes to Snowflake via JDBC connector
- Bulk loading for historical data migration

## Troubleshooting

### Common Issues

1. **Connection Timeout**
   ```
   Solution: Check network connectivity and firewall settings
   Verify account identifier format: account.region.cloud
   ```

2. **Permission Denied**
   ```
   Solution: Ensure user has required privileges
   Grant necessary roles: SYSADMIN, USAGE on warehouses
   ```

3. **Query Performance**
   ```
   Solution: Check clustering keys and query patterns
   Consider micro-partitioning and data compression
   Monitor warehouse sizing and concurrency
   ```

### Support Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [SQL Reference](https://docs.snowflake.com/en/sql-reference.html)
- [Performance Tuning](https://docs.snowflake.com/en/user-guide/performance-tuning.html)
