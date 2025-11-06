# Troubleshooting

Common issues and solutions for the sales data pipeline.

## Quick Diagnostics

Run the comprehensive health check:

```bash
python tools/validation/setup_doctor.py
```

This checks:
- Environment variables
- AWS connectivity
- Snowflake connectivity
- S3 buckets
- Docker services
- Airflow status
- Data layer health

## Environment and Configuration

### Error: "Missing required environment variable"

**Cause:** `.env` file missing or incomplete

**Solution:**
```bash
# Copy template
cp .env.example .env

# Edit with your credentials
nano .env

# Validate configuration
python tools/validation/config_validator.py --validate-all
```

### Error: "Invalid AWS credentials"

**Cause:** Incorrect or expired AWS credentials

**Solution:**
```bash
# Verify credentials
aws sts get-caller-identity

# Check .env file
grep AWS_ACCESS_KEY_ID .env
grep AWS_SECRET_ACCESS_KEY .env

# Test S3 access
aws s3 ls
```

If credentials are invalid:
- Generate new access keys in AWS IAM Console
- Update `.env` with new credentials
- Restart Docker services: `docker-compose restart`

### Error: "Snowflake authentication failed"

**Cause:** Incorrect Snowflake credentials or account name

**Solution:**
```bash
# Test connection with SnowSQL
snowsql -a your_account.region -u your_username

# Verify account name format
# Correct: xy12345.us-east-1
# Incorrect: xy12345 (missing region)

# Check .env file
grep SNOWFLAKE .env
```

Common fixes:
- Account name must include region: `account.region`
- Use ACCOUNTADMIN or SYSADMIN role
- Verify password has no special shell characters (use quotes in .env)

### Error: "Terraform state locked"

**Cause:** Previous Terraform operation interrupted

**Solution:**
```bash
cd infrastructure/terraform

# Force unlock (use lock ID from error message)
terraform force-unlock <LOCK_ID>

# If unsuccessful, delete lock manually in DynamoDB (if using remote state)
```

## Docker and Services

### Error: "Port 8080 already in use"

**Cause:** Another service using port 8080

**Solution:**
```bash
# Find process using port
lsof -i :8080

# Kill process
kill -9 <PID>

# Or change Airflow port in docker-compose.yml
# Change: "8080:8080" to "8081:8080"
```

### Error: "Cannot connect to Docker daemon"

**Cause:** Docker Desktop not running

**Solution:**
- Start Docker Desktop
- Wait for Docker to fully initialize
- Verify: `docker info`

### Error: "Service unhealthy" in docker-compose ps

**Cause:** Service failing health checks

**Solution:**
```bash
# Check specific service logs
docker-compose logs [service-name]

# Common services to check:
docker-compose logs postgres
docker-compose logs airflow-webserver
docker-compose logs kafka

# Restart specific service
docker-compose restart [service-name]

# Full restart
docker-compose down
docker-compose up -d
```

### Error: "Container exited with code 137"

**Cause:** Out of memory

**Solution:**
- Increase Docker memory in Docker Desktop settings
- Minimum: 4GB, Recommended: 8GB
- On Linux, adjust Docker daemon memory limits

### Services won't start after update

**Solution:**
```bash
# Clean rebuild
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

**Warning:** `-v` flag removes all volumes and data

## Airflow Issues

### DAGs not appearing in UI

**Cause:** DAG parsing errors or slow initialization

**Solution:**
```bash
# Check scheduler logs
docker-compose logs airflow-scheduler | grep ERROR

# Verify DAG files exist
docker-compose exec airflow-webserver ls /opt/airflow/dags

# Test DAG parsing
docker-compose exec airflow-webserver airflow dags list

# Force DAG refresh
docker-compose restart airflow-scheduler
```

Wait 2-3 minutes after restart for DAGs to appear.

### Task stuck in "running" state

**Cause:** Task hung or crashed without updating status

**Solution:**
```bash
# Mark task as failed (allows retry)
docker-compose exec airflow-webserver airflow tasks clear \
  batch_processing_dag \
  process_batch_data \
  -s 2024-01-15 \
  -e 2024-01-15

# Or kill zombie tasks
docker-compose exec airflow-webserver airflow tasks clear \
  --only-failed \
  --yes \
  batch_processing_dag
```

### Error: "Broken DAG" icon in UI

**Cause:** Python syntax error in DAG file

**Solution:**
```bash
# Check specific DAG for errors
docker-compose exec airflow-webserver python /opt/airflow/dags/batch_processing_dag.py

# Check scheduler logs for detailed error
docker-compose logs airflow-scheduler | tail -50
```

Fix Python errors in DAG file and save. Airflow auto-reloads.

### Airflow UI slow or unresponsive

**Cause:** Too many DAG runs or task instances

**Solution:**
```bash
# Clean old DAG runs (keeps last 30 days)
docker-compose exec airflow-webserver airflow db clean --clean-before-timestamp "2024-01-01" --yes

# Or adjust retention in airflow.cfg
# [core]
# max_active_runs_per_dag = 16
```

## Data Processing Issues

### Batch pipeline not detecting files

**Cause:** Incorrect S3 path or partition format

**Solution:**
```bash
# Verify Bronze structure
aws s3 ls s3://your-project-raw-bucket/sales_data/ --recursive

# Expected format: year=YYYY/month=MM/day=DD/
# Incorrect: 2024/01/15/ (missing key= prefix)

# Check task logs
# In Airflow UI: batch_processing_dag → check_new_files → View Log

# Verify .env bucket names
grep RAW_BUCKET .env
grep PROCESSED_BUCKET .env
```

### Spark job failing with "OutOfMemoryError"

**Cause:** Insufficient memory for data volume

**Solution:**

Increase Spark memory in `src/spark/jobs/batch_etl.py`:
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()
```

Or process in smaller batches:
```python
df = spark.read.csv(input_path) \
    .repartition(200)  # More partitions = less memory per partition
```

### Error: "CSV schema mismatch"

**Cause:** CSV file has incorrect number or order of fields

**Solution:**
```bash
# Check CSV structure
head -1 data/your_file.csv

# Compare with expected schema (24 fields):
# Row ID,Order ID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,
# Segment,City,State,Country,Postal Code,Market,Region,Product ID,Category,
# Sub-Category,Product Name,Sales,Quantity,Discount,Profit,Shipping Cost,Order Priority

# Validate manually
python -c "
import csv
with open('data/your_file.csv') as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames
    print(f'Field count: {len(headers)}')
    print('Headers:', headers)
"
```

Fix CSV structure or use schema evolution in Spark.

### dbt tests failing

**Cause:** Data quality issues or connection problems

**Solution:**
```bash
# Run dbt tests manually to see detailed errors
cd dbt/
dbt test --target dev

# Run specific test
dbt test --select stg_sales_silver

# Check which tests failed
# Look for TEST FAILED in output

# View test SQL
cat tests/singular/test_business_logic_validation.sql

# Test Snowflake connection
dbt debug --target dev
```

Common dbt test failures:
- Null values in required fields: Check Silver data quality
- Referential integrity: Ensure dimension keys exist
- Duplicate records: Check deduplication logic

### Delta Lake "ConcurrentModificationException"

**Cause:** Multiple processes writing to same Delta table simultaneously

**Solution:**

Enable optimistic concurrency control:
```python
df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .option("optimizeWrite", "true") \
    .mode("append") \
    .save(output_path)
```

Or use Delta Lake merge for upserts:
```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, output_path)
deltaTable.alias("target").merge(
    df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

## Kafka and Streaming Issues

### Kafka producer can't connect

**Cause:** Kafka not running or network issue

**Solution:**
```bash
# Verify Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka | grep ERROR

# Test connectivity
telnet localhost 9092

# Verify topic exists
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Create topic if missing
docker-compose exec kafka kafka-topics \
  --create \
  --topic sales_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Kafka Connect not writing to S3

**Cause:** Connector misconfigured or S3 permissions issue

**Solution:**
```bash
# Check connector status
curl http://localhost:8083/connectors/s3-sink-sales/status

# If FAILED, check error message
curl http://localhost:8083/connectors/s3-sink-sales/status | jq '.tasks[0].trace'

# Check connector logs
docker-compose logs kafka-connect

# Common fixes:
# 1. Verify S3 bucket exists and accessible
aws s3 ls s3://your-project-raw-bucket/

# 2. Verify AWS credentials in connector config
# 3. Redeploy connector
bash deploy/streaming/deploy_connector.sh
```

### No streaming data in Silver

**Cause:** Bronze files not processed or wrong path

**Solution:**
```bash
# Verify Bronze files exist
aws s3 ls s3://your-project-raw-bucket/sales_data/sales_events/ --recursive

# Check streaming DAG logs
# Airflow UI: streaming_processing_dag → check_streaming_data → View Log

# Verify path in DAG matches Bronze structure
# Expected: sales_data/sales_events/year=YYYY/month=MM/day=DD/

# Manually trigger DAG
docker-compose exec airflow-webserver airflow dags trigger streaming_processing_dag
```

## Snowflake Issues

### Error: "SQL compilation error"

**Cause:** SQL syntax error in dbt model

**Solution:**
```bash
# Run dbt with debug output
cd dbt/
dbt run --target dev --debug

# Check compiled SQL
cat target/compiled/sales_pipeline/models/marts/fact_sales.sql

# Test SQL directly in Snowflake
snowsql -a your_account -u your_username -q "SELECT * FROM ..."
```

### Error: "Parquet file ... was inaccessible" when dbt runs on Delta Direct

**Symptoms:**
- dbt models (for example `dim_customer`) fail with `Database Error ... parquet file 'sales/part-....snappy.parquet' was inaccessible`
- Snowflake keeps requesting files that were deleted from the Silver S3 location

**Cause:** The Delta Direct Iceberg table still references parquet files that were removed when the Silver bucket was cleaned manually. A normal `ALTER ICEBERG TABLE ... REFRESH` cannot drop those stale references.

**Fix:**
1. Stop any jobs writing to the Silver layer.
2. (Optional) Remove the entire Silver Delta directory at `s3://<processed-bucket>/silver/sales/` if you still need to reset it.
3. Recreate the Iceberg table so Snowflake drops the old metadata. Choose one:
   - Python helper:
     ```bash
     cd deploy/snowflake
     python setup_delta_direct.py destroy
     python setup_delta_direct.py
     ```
   - Manual SQL:
     ```sql
     USE DATABASE SALES_DW;
     USE SCHEMA RAW;
     DROP ICEBERG TABLE IF EXISTS SALES_SILVER_EXTERNAL;
     CREATE ICEBERG TABLE SALES_SILVER_EXTERNAL
         EXTERNAL_VOLUME = 'S3_SILVER_VOLUME'
         CATALOG = 'DELTA_CATALOG'
         BASE_LOCATION = 'sales/'
         COMMENT = 'Unified batch+streaming sales data from Delta Lake Silver layer via catalog integration';
     GRANT SELECT ON ICEBERG TABLE SALES_SILVER_EXTERNAL TO ROLE ANALYTICS_ROLE;
     ```
4. Re-run the Spark batch ETL so the Delta table is rebuilt with fresh metadata (dedup logic included).
5. Refresh the Iceberg pointer:
   ```sql
   ALTER ICEBERG TABLE SALES_SILVER_EXTERNAL REFRESH;
   ```
6. Rerun the analytics/dbt job. Snowflake will now read only the new parquet files and the star schema build should succeed.

### Tables not appearing in Snowflake

**Cause:** dbt run failed or wrong database/schema

**Solution:**
```sql
-- Verify database and schema
SHOW DATABASES;
USE DATABASE SALES_DB;
SHOW SCHEMAS;
USE SCHEMA PUBLIC;

-- List tables
SHOW TABLES;

-- Check dbt run logs
-- Airflow UI: analytics_dag → dbt_run → View Log
```

### Snowflake warehouse suspended

**Cause:** Warehouse auto-suspend after inactivity

**Solution:**
```sql
-- Resume warehouse
ALTER WAREHOUSE COMPUTE_WH RESUME;

-- Or adjust auto-suspend
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 600;  -- 10 minutes
```

## Performance Issues

### Pipeline running slow

**Diagnostic steps:**
```bash
# 1. Check resource usage
docker stats

# 2. Check Spark job progress
# Airflow UI: View task log for "Records processed" metrics

# 3. Check data volume
aws s3 ls s3://your-project-raw-bucket/sales_data/ --recursive --summarize

# 4. Check Snowflake query performance
# In Snowflake UI: History tab → View slow queries
```

**Common optimizations:**
- Increase Spark partitions for large datasets
- Enable Snowflake query result caching
- Add partition pruning in dbt models
- Use Snowflake clustering keys on large tables

### High AWS costs

**Solution:**
```bash
# 1. Check S3 storage usage
aws s3 ls s3://your-project-raw-bucket/ --recursive --summarize

# 2. Implement lifecycle policies
# Delete Bronze files older than 90 days
# Move Silver to Glacier after 180 days

# 3. Use Snowflake auto-suspend
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;

# 4. Monitor Databricks cluster usage
# Ensure clusters auto-terminate after 30 min idle
```

## Getting Help

If issues persist:

1. **Check service logs:**
   ```bash
   docker-compose logs [service-name] | tail -100
   ```

2. **Run validation:**
   ```bash
   python tools/validation/setup_doctor.py
   python tools/validation/config_validator.py --validate-all
   ```

3. **Verify infrastructure:**
   ```bash
   cd infrastructure/terraform
   terraform plan  # Should show no changes if healthy
   ```

4. **Review documentation:**
   - [Architecture](../reference/architecture.md) - System design
   - [Configuration](../reference/configuration.md) - All settings
   - [Getting Started](../tutorial/getting-started.md) - Setup walkthrough

5. **Collect diagnostic information:**
   ```bash
   # Environment
   env | grep -E "AWS|SNOWFLAKE|KAFKA" > diagnostics.txt

   # Docker status
   docker-compose ps >> diagnostics.txt

   # Recent logs
   docker-compose logs --tail=100 >> diagnostics.txt

   # Terraform state
   cd infrastructure/terraform && terraform show >> ../../diagnostics.txt
   ```
