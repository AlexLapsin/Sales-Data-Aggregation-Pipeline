# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Data ETL Pipeline - Databricks Version
# MAGIC
# MAGIC This notebook processes sales CSV files from S3 and loads them into Snowflake.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Configure Snowflake connection in cluster libraries
# MAGIC - Set up S3 access (IAM role or access keys)
# MAGIC - Install required packages in cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Setup

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    when,
    trim,
    upper,
    input_file_name,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from datetime import datetime, timezone

# COMMAND ----------

# Configuration - Set these in Databricks secrets or widgets
dbutils.widgets.text("input_path", "s3://your-bucket/sales-data/", "Input S3 Path")
dbutils.widgets.text("output_table", "SALES_BATCH_RAW", "Output Table")
dbutils.widgets.text("batch_id", "", "Batch ID (optional)")

# Get widget values
input_path = dbutils.widgets.get("input_path")
output_table = dbutils.widgets.get("output_table")
batch_id = (
    dbutils.widgets.get("batch_id")
    or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

print(f"Input Path: {input_path}")
print(f"Output Table: {output_table}")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Snowflake Connection Setup

# COMMAND ----------

# Snowflake connection options - use Databricks secrets for production
snowflake_options = {
    "sfUrl": dbutils.secrets.get(
        "snowflake", "account"
    ),  # or use dbutils.secrets.get()
    "sfUser": dbutils.secrets.get("snowflake", "user"),
    "sfPassword": dbutils.secrets.get("snowflake", "password"),
    "sfDatabase": "SALES_DW",
    "sfSchema": "RAW",
    "sfWarehouse": "ETL_WH",
    "sfRole": "SYSADMIN",
}

# Alternative: Use environment variables (less secure)
# snowflake_options = {
#     "sfUrl": "YOUR_ACCOUNT.us-east-1.aws",
#     "sfUser": "YOUR_USER",
#     "sfPassword": "YOUR_PASSWORD",
#     "sfDatabase": "SALES_DW",
#     "sfSchema": "RAW",
#     "sfWarehouse": "ETL_WH",
#     "sfRole": "SYSADMIN"
# }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Schema and Read Data

# COMMAND ----------

# Define schema for sales CSV files
sales_schema = StructType(
    [
        StructField("Row ID", IntegerType(), True),
        StructField("Order ID", StringType(), False),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal Code", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True),
    ]
)

# COMMAND ----------

# Read CSV files from S3
print(f"Reading CSV files from: {input_path}")

raw_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .option("dateFormat", "MM/dd/yyyy")
    .schema(sales_schema)
    .csv(input_path)
    .withColumn("source_file", input_file_name())
)

record_count = raw_df.count()
print(f"Successfully read {record_count} records from CSV files")

# Display sample data
display(raw_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Cleaning and Transformation

# COMMAND ----------

# Clean and transform the data
print("Starting data cleaning and transformation")

cleaned_df = (
    raw_df
    # Remove rows with null order IDs
    .filter(col("Order ID").isNotNull())
    # Clean and standardize text fields
    .withColumn("Order ID", trim(col("Order ID")))
    .withColumn("Customer ID", trim(col("Customer ID")))
    .withColumn("Product ID", trim(col("Product ID")))
    .withColumn("Category", trim(upper(col("Category"))))
    .withColumn("Region", trim(upper(col("Region"))))
    .withColumn("Country", trim(upper(col("Country"))))
    .withColumn("State", trim(upper(col("State"))))
    .withColumn("City", trim(col("City")))
    # Parse dates properly
    .withColumn("Order Date", to_date(col("Order Date"), "MM/dd/yyyy"))
    .withColumn("Ship Date", to_date(col("Ship Date"), "MM/dd/yyyy"))
    # Handle numeric fields
    .withColumn(
        "Sales",
        when(col("Sales").isNull() | (col("Sales") < 0), 0.0).otherwise(col("Sales")),
    )
    .withColumn(
        "Quantity",
        when(col("Quantity").isNull() | (col("Quantity") <= 0), 1).otherwise(
            col("Quantity")
        ),
    )
    .withColumn(
        "Discount",
        when(col("Discount").isNull() | (col("Discount") < 0), 0.0)
        .when(col("Discount") > 1.0, col("Discount") / 100)
        .otherwise(col("Discount")),
    )
    .withColumn("Profit", when(col("Profit").isNull(), 0.0).otherwise(col("Profit")))
    # Calculate derived fields
    .withColumn(
        "unit_price",
        when(col("Quantity") > 0, col("Sales") / col("Quantity")).otherwise(0.0),
    )
    .withColumn(
        "profit_margin",
        when(col("Sales") > 0, col("Profit") / col("Sales")).otherwise(0.0),
    )
    # Add metadata fields
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("BATCH"))
    .withColumn("batch_id", lit(batch_id))
    .withColumn("partition_date", date_format(current_timestamp(), "yyyy-MM-dd"))
)

# Filter out invalid records
final_df = cleaned_df.filter(
    col("Order Date").isNotNull()
    & col("Sales").isNotNull()
    & (col("Sales") >= 0)
    & col("Quantity").isNotNull()
    & (col("Quantity") > 0)
)

initial_count = raw_df.count()
final_count = final_df.count()
filtered_count = initial_count - final_count

print(f"Data cleaning completed:")
print(f"  - Initial records: {initial_count}")
print(f"  - Valid records: {final_count}")
print(f"  - Filtered out: {filtered_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Map to Snowflake Schema

# COMMAND ----------

# Map DataFrame columns to match Snowflake table schema
snowflake_df = final_df.select(
    col("Order ID").alias("ORDER_ID"),
    col("Customer ID").alias("STORE_ID"),
    col("Product ID").alias("PRODUCT_ID"),
    col("Product Name").alias("PRODUCT_NAME"),
    col("Category").alias("CATEGORY"),
    col("Quantity").alias("QUANTITY"),
    col("unit_price").alias("UNIT_PRICE"),
    col("Sales").alias("TOTAL_PRICE"),
    col("Order Date").alias("ORDER_DATE"),
    col("Ship Date").alias("SHIP_DATE"),
    col("Sales").alias("SALES"),
    col("Profit").alias("PROFIT"),
    col("Segment").alias("CUSTOMER_SEGMENT"),
    col("Region").alias("REGION"),
    col("Country").alias("COUNTRY"),
    col("State").alias("STATE"),
    col("City").alias("CITY"),
    col("source_file").alias("SOURCE_FILE"),
    col("batch_id").alias("BATCH_ID"),
    col("ingestion_timestamp").alias("INGESTION_TIMESTAMP"),
    col("source_system").alias("SOURCE_SYSTEM"),
    col("partition_date").cast("date").alias("PARTITION_DATE"),
)

print(f"Prepared {snowflake_df.count()} records for Snowflake")
display(snowflake_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Snowflake

# COMMAND ----------

print(f"Writing {snowflake_df.count()} records to Snowflake table: {output_table}")

# Write to Snowflake
(
    snowflake_df.write.format("snowflake")
    .options(**snowflake_options)
    .option("dbtable", output_table)
    .mode("append")
    .save()
)

print(f"Successfully wrote data to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Checks

# COMMAND ----------

# Basic data quality checks
print("=== DATA QUALITY SUMMARY ===")
print(f"Total records processed: {snowflake_df.count()}")
print(f"Unique order IDs: {snowflake_df.select('ORDER_ID').distinct().count()}")
print(
    f"Date range: {snowflake_df.agg({'ORDER_DATE': 'min'}).collect()[0][0]} to {snowflake_df.agg({'ORDER_DATE': 'max'}).collect()[0][0]}"
)
print(f"Categories: {snowflake_df.select('CATEGORY').distinct().count()}")
print(f"Regions: {snowflake_df.select('REGION').distinct().count()}")

# Show category breakdown
print("\n=== CATEGORY BREAKDOWN ===")
display(snowflake_df.groupBy("CATEGORY").count().orderBy("count", ascending=False))

# Show region breakdown
print("\n=== REGION BREAKDOWN ===")
display(snowflake_df.groupBy("REGION").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Completion Summary

# COMMAND ----------

print("=== ETL JOB COMPLETED ===")
print(f"Batch ID: {batch_id}")
print(f"Input Path: {input_path}")
print(f"Output Table: {output_table}")
print(f"Records Processed: {snowflake_df.count()}")
print(f"Job completed at: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Verify Data in Snowflake**:
# MAGIC    ```sql
# MAGIC    SELECT COUNT(*) FROM SALES_DW.RAW.SALES_BATCH_RAW
# MAGIC    WHERE BATCH_ID = 'your_batch_id';
# MAGIC    ```
# MAGIC
# MAGIC 2. **Run dbt Transformations**:
# MAGIC    - Execute dbt models to create dimension and fact tables
# MAGIC    - Update data marts for analytics
# MAGIC
# MAGIC 3. **Schedule in Airflow**:
# MAGIC    - Add this job to your Airflow DAG
# MAGIC    - Set up monitoring and alerts
