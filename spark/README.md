# Spark ETL Jobs

This directory contains PySpark ETL jobs for processing batch sales data from S3 and loading it into Snowflake.

## Overview

The Spark ETL pipeline processes CSV files containing sales data, applies data cleaning and transformations, and loads the results into Snowflake data warehouse. It's designed to work with multiple Spark deployment options.

## Components

### 1. Main ETL Job (`sales_batch_job.py`)
- Production-ready PySpark job for sales data processing
- Configurable input/output paths and Snowflake connections
- Comprehensive data cleaning and validation
- Error handling and logging

### 2. Databricks Notebook (`notebooks/Sales_ETL_Databricks.py`)
- Interactive notebook version for Databricks platform
- Step-by-step execution with visualizations
- Built-in data quality checks

### 3. Configuration (`spark_config.py`)
- Environment-specific Spark configurations
- Snowflake connection management
- Resource allocation settings

### 4. Testing (`test_spark_job.py`)
- Local testing with sample data generation
- Unit tests for transformation logic
- Configuration validation

## Quick Start

### Local Development

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables:**
   ```bash
   export SNOWFLAKE_ACCOUNT="your-account.region.cloud"
   export SNOWFLAKE_USER="your-username"
   export SNOWFLAKE_PASSWORD="your-password"
   export SNOWFLAKE_DATABASE="SALES_DW"
   export SNOWFLAKE_SCHEMA="RAW"
   export SNOWFLAKE_WAREHOUSE="ETL_WH"
   ```

3. **Test locally:**
   ```bash
   python test_spark_job.py
   ```

4. **Run ETL job:**
   ```bash
   spark-submit --packages net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.11.3 \
                sales_batch_job.py \
                --input-path file:///path/to/csv/files/ \
                --output-table SALES_BATCH_RAW
   ```

### Databricks Deployment

1. **Upload notebook:**
   - Import `notebooks/Sales_ETL_Databricks.py` to your Databricks workspace

2. **Configure cluster:**
   - Install Snowflake Spark connector: `net.snowflake:spark-snowflake_2.12:2.11.3`
   - Set up S3 access (IAM role or access keys)

3. **Set up secrets:**
   ```bash
   # Create secret scope
   databricks secrets create-scope snowflake

   # Add secrets
   databricks secrets put-secret snowflake account
   databricks secrets put-secret snowflake user
   databricks secrets put-secret snowflake password
   ```

4. **Run notebook:**
   - Set widget parameters for input path and table name
   - Execute all cells

### AWS EMR Deployment

1. **Create EMR cluster with Spark:**
   ```bash
   aws emr create-cluster \
       --name "Sales-ETL-Cluster" \
       --applications Name=Spark \
       --ec2-attributes KeyName=your-key \
       --instance-type m5.xlarge \
       --instance-count 3
   ```

2. **Submit job:**
   ```bash
   spark-submit --master yarn \
                --deploy-mode cluster \
                --packages net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.11.3 \
                s3://your-bucket/spark/sales_batch_job.py \
                --input-path s3://your-bucket/sales-data/ \
                --output-table SALES_BATCH_RAW
   ```

## Data Processing Pipeline

### Input Data Format
Expected CSV schema:
```
Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Customer Name,
Segment, Country, City, State, Postal Code, Region, Product ID, Category,
Sub-Category, Product Name, Sales, Quantity, Discount, Profit
```

### Transformations Applied

1. **Data Cleaning:**
   - Remove null order IDs
   - Standardize text fields (trim, uppercase)
   - Parse dates with error handling
   - Handle negative/null numeric values

2. **Data Validation:**
   - Filter invalid records (negative sales, zero quantity)
   - Validate date ranges
   - Check required fields

3. **Derived Fields:**
   - `unit_price` = Sales / Quantity
   - `profit_margin` = Profit / Sales

4. **Metadata Addition:**
   - `ingestion_timestamp`
   - `source_system` = "BATCH"
   - `batch_id` (unique per run)
   - `partition_date`

### Output Schema (Snowflake)
Mapped to `SALES_BATCH_RAW` table with uppercase column names:
- ORDER_ID, STORE_ID, PRODUCT_ID, PRODUCT_NAME, CATEGORY
- QUANTITY, UNIT_PRICE, TOTAL_PRICE, ORDER_DATE, SHIP_DATE
- SALES, PROFIT, CUSTOMER_SEGMENT, REGION, COUNTRY, STATE, CITY
- SOURCE_FILE, BATCH_ID, INGESTION_TIMESTAMP, SOURCE_SYSTEM, PARTITION_DATE

## Configuration

### Environment Variables
```bash
# Snowflake Connection
SNOWFLAKE_ACCOUNT="account.region.cloud"
SNOWFLAKE_USER="username"
SNOWFLAKE_PASSWORD="password"
SNOWFLAKE_DATABASE="SALES_DW"
SNOWFLAKE_SCHEMA="RAW"
SNOWFLAKE_WAREHOUSE="ETL_WH"
SNOWFLAKE_ROLE="SYSADMIN"

# AWS (if using S3)
AWS_ACCESS_KEY_ID="your-access-key"
AWS_SECRET_ACCESS_KEY="your-secret-key"
AWS_DEFAULT_REGION="us-east-1"
```

### Spark Configuration Options

**Local Development:**
- Master: `local[*]`
- Driver Memory: 1GB
- Executor Memory: 2GB

**Databricks:**
- Managed cluster configuration
- Auto-scaling enabled
- Optimized for cloud storage

**EMR:**
- Driver Memory: 4GB
- Executor Memory: 8GB
- Dynamic allocation: 2-20 executors

## Monitoring and Logging

### Application Logs
```bash
# View Spark application logs
yarn logs -applicationId application_xxx

# Databricks logs available in cluster UI
# Local logs printed to console
```

### Data Quality Metrics
- Records processed vs. filtered
- Unique order ID count
- Date range validation
- Category/region distribution

### Performance Monitoring
- Job execution time
- Resource utilization
- Memory usage patterns
- S3/Snowflake I/O performance

## Error Handling

### Common Issues

1. **Snowflake Connection Failures:**
   - Verify credentials and network access
   - Check warehouse status
   - Validate URL format

2. **S3 Access Issues:**
   - Verify AWS credentials
   - Check bucket permissions
   - Ensure region configuration

3. **Schema Mismatches:**
   - Validate CSV column names
   - Check data types
   - Review date formats

4. **Memory Issues:**
   - Increase driver/executor memory
   - Optimize partition sizes
   - Enable dynamic allocation

### Troubleshooting Commands

```bash
# Test Snowflake connectivity
python -c "from spark_config import SnowflakeConfig; SnowflakeConfig.from_env()"

# Validate input data
python test_spark_job.py

# Check Spark configuration
spark-submit --version
```

## Performance Tuning

### Spark Optimizations
- Enable adaptive query execution
- Use appropriate partition sizes
- Configure memory fractions
- Enable column pruning

### Snowflake Optimizations
- Use appropriate warehouse size
- Enable result caching
- Optimize table clustering
- Use proper data types

### S3 Optimizations
- Use S3A filesystem
- Enable fast upload
- Configure multipart uploads
- Use appropriate block sizes

## Integration with Pipeline

This Spark job integrates with the broader data pipeline:

1. **Triggered by Airflow DAG**
2. **Reads from S3 data lake**
3. **Writes to Snowflake raw tables**
4. **Followed by dbt transformations**
5. **Results in analytics-ready data marts**

## Next Steps

1. **Add Delta Lake support** for better data management
2. **Implement data lineage tracking**
3. **Add more sophisticated data quality checks**
4. **Create reusable transformation functions**
5. **Add support for streaming data processing**
