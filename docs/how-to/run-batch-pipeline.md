# Run Batch Pipeline

Execute CSV batch ingestion through Bronze, Silver, and Gold layers.

## Prerequisites

- Infrastructure deployed
- Environment configured
- Docker services running
- Airflow UI accessible at http://localhost:8080

## Pipeline Architecture

```
CSV Files → S3 Bronze → Spark ETL → Delta Silver → dbt → Snowflake Gold
```

**Execution flow:**
1. Upload CSV files to Bronze S3 (partitioned by date)
2. Airflow detects new files
3. Spark processes files to Silver Delta Lake
4. dbt transforms Silver to Gold dimensional model
5. Snowflake loads final analytics tables

## Upload Sample Data

The project includes sample sales data. Upload to Bronze layer:

```bash
python src/bronze/data_uploader.py --source data/sample_sales.csv
```

Options:
- `--source` - Path to CSV file or directory
- `--partition-date` - Custom partition date (default: today)

Expected output:
```
Loading CSV: data/sample_sales.csv
Validating schema... PASS
Partitioning by date: 2024-01-15
Uploading to s3://your-project-raw-bucket/sales_data/year=2024/month=01/day=15/
Successfully uploaded 1 file(s)
```

Verify upload:

```bash
aws s3 ls s3://your-project-raw-bucket/sales_data/ --recursive
```

Expected output:
```
2024-01-15 10:30:00  12345 sales_data/year=2024/month=01/day=15/sample_sales.csv
```

## Upload Custom Data

Upload your own CSV files:

```bash
python src/bronze/data_uploader.py --source /path/to/your/sales.csv
```

**CSV schema requirements (24 fields):**
```
Row ID, Order ID, Order Date, Ship Date, Ship Mode, Customer ID, Customer Name,
Segment, City, State, Country, Postal Code, Market, Region, Product ID, Category,
Sub-Category, Product Name, Sales, Quantity, Discount, Profit, Shipping Cost, Order Priority
```

Schema validation runs automatically. If validation fails:
- Check field count (must be exactly 24 columns)
- Check field names match expected schema
- Check data types (dates as YYYY-MM-DD, numbers as numeric)

## Enable Batch Processing DAG

Access Airflow UI at http://localhost:8080 and login (admin/admin).

Enable the DAG:
1. Find `batch_processing_dag` in DAG list
2. Toggle the switch to enable (turns blue)
3. Verify schedule: Runs every 15 minutes

## Trigger DAG Manually

Option 1 - Via UI:
1. Click the play button (▶) on `batch_processing_dag`
2. Select "Trigger DAG"
3. Optionally add configuration JSON (leave empty for defaults)
4. Click "Trigger"

Option 2 - Via CLI:

```bash
docker-compose exec airflow-webserver airflow dags trigger batch_processing_dag
```

Expected output:
```
Created <DagRun batch_processing_dag @ 2024-01-15T10:30:00+00:00: manual__2024-01-15T10:30:00+00:00, state:queued, queued_at: 2024-01-15 10:30:00>
```

## Monitor DAG Execution

Click on DAG name to view execution details.

**Graph View:**
- Shows task dependencies
- Green = Success, Red = Failed, Yellow = Running

**Task sequence:**
1. `check_new_files` - Scans Bronze S3 for new files
2. `process_batch_data` - Executes Spark ETL to Silver
3. `trigger_analytics` - Triggers dbt transformation DAG

Execution time: 5-8 minutes (depends on data volume)

**Task Logs:**

Click any task to view logs:
- `check_new_files` logs: List of detected files
- `process_batch_data` logs: Spark job progress, records processed
- `trigger_analytics` logs: DAG trigger confirmation

## Verify Bronze Layer

Check files exist in Bronze S3:

```bash
aws s3 ls s3://your-project-raw-bucket/sales_data/ --recursive
```

Expected structure:
```
sales_data/
├── year=2024/
│   └── month=01/
│       └── day=15/
│           └── sample_sales.csv
```

Count records in Bronze:

```bash
aws s3 cp s3://your-project-raw-bucket/sales_data/year=2024/month=01/day=15/sample_sales.csv - | wc -l
```

## Verify Silver Layer

Check Delta Lake files in Silver S3:

```bash
aws s3 ls s3://your-project-processed-bucket/silver/sales/ --recursive
```

Expected structure:
```
silver/sales/
├── _delta_log/
│   ├── 00000000000000000000.json
│   └── 00000000000000000001.json
├── part-00000-xxx.snappy.parquet
└── part-00001-xxx.snappy.parquet
```

The `_delta_log/` directory contains transaction logs for ACID compliance.

Query Silver data using PySpark:

```python
from delta import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifySilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("s3://your-project-processed-bucket/silver/sales/")
print(f"Silver record count: {df.count()}")
df.show(5)
```

## Verify Gold Layer

The `analytics_dag` is automatically triggered after Silver processing completes.

Monitor analytics DAG execution:
1. Find `analytics_dag` in Airflow UI
2. Check most recent run
3. Verify all dbt tasks completed successfully

**dbt tasks:**
1. `dbt_test` - Data quality tests on Silver layer
2. `dbt_run` - Runs dimensional model transformations
3. `dbt_test_marts` - Data quality tests on Gold layer

Query Gold tables in Snowflake:

```sql
USE DATABASE SALES_DB;
USE SCHEMA PUBLIC;

-- Check record counts
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;

-- Sample sales fact
SELECT * FROM fact_sales LIMIT 10;

-- Sales by category
SELECT
    p.category,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.profit) AS total_profit
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;
```

## Re-run Pipeline with New Data

Upload additional CSV files:

```bash
python src/bronze/data_uploader.py --source data/new_sales_batch.csv
```

The DAG automatically detects new files on next scheduled run (every 15 minutes).

To process immediately, trigger the DAG manually.

## Batch Processing Configuration

Customize batch processing in `airflow/dags/batch_processing_dag.py`:

**Change schedule:**
```python
schedule_interval="*/15 * * * *",  # Every 15 minutes (default)
schedule_interval="0 2 * * *",     # Daily at 2 AM
schedule_interval="@hourly",       # Every hour
```

**Change Spark configuration:**
```python
spark_config = {
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "200"
}
```

After changes, restart Airflow:

```bash
docker-compose restart airflow-webserver airflow-scheduler
```

## Performance Tuning

**For large CSV files (>1GB):**

1. Increase Spark resources in `src/spark/jobs/batch_etl.py`:
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

2. Enable file partitioning:
```python
df.write \
    .partitionBy("order_date") \
    .format("delta") \
    .mode("append") \
    .save(output_path)
```

3. Adjust parallelism:
```python
df = spark.read.csv(input_path, header=True, inferSchema=True) \
    .repartition(100)  # Increase partitions for larger datasets
```

**For high-frequency uploads:**

Change DAG schedule to run more frequently:
```python
schedule_interval="*/5 * * * *",  # Every 5 minutes
```

## Troubleshooting

**DAG not detecting new files:**
- Verify Bronze S3 path: `aws s3 ls s3://your-project-raw-bucket/sales_data/`
- Check task logs in `check_new_files` task
- Verify partition format: `year=YYYY/month=MM/day=DD`

**Spark job failing:**
- Check logs: Click `process_batch_data` task → View Log
- Common issues:
  - CSV schema mismatch (check field count and names)
  - S3 permissions (verify IAM role has read/write access)
  - Memory errors (increase Spark executor memory)

**dbt transformations failing:**
- Check dbt logs in `analytics_dag` tasks
- Verify Snowflake connection: `docker-compose exec airflow-webserver snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER`
- Run dbt tests manually: `cd dbt && dbt test --target dev`

**Delta Lake corruption:**
- Check transaction log: `aws s3 ls s3://your-project-processed-bucket/silver/sales/_delta_log/`
- Repair table: Use Delta Lake repair utilities
- Last resort: Delete Silver directory and re-process Bronze data

## Next Steps

- [Run Streaming Pipeline](run-streaming-pipeline.md) - Add real-time data ingestion
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Architecture](../reference/architecture.md) - Deep dive into pipeline design
