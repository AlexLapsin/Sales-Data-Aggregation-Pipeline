#!/usr/bin/env python3
"""
PySpark ETL Job for Sales Data Batch Processing

This job processes CSV files from S3, transforms the data, and loads it into Snowflake.
Designed to run on Databricks or standalone Spark clusters.

Usage:
    spark-submit --packages net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:3.1.4 \
                 sales_batch_job.py --input-path s3a://bucket/path/ --output-table SALES_BATCH_RAW
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame
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


class SalesETLJob:
    """PySpark ETL job for processing sales data from S3 to Snowflake"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Snowflake connection properties using environment variables
        # Compatible with both local execution and Databricks full edition
        account_name = os.getenv("SNOWFLAKE_ACCOUNT_NAME")
        org_name = os.getenv("SNOWFLAKE_ORGANIZATION_NAME")
        snowflake_url = (
            f"{org_name}-{account_name}.snowflakecomputing.com"
            if account_name and org_name
            else None
        )

        self.snowflake_options = {
            "sfUrl": snowflake_url,
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE"),
            # FIXED: Conservative timeout configuration to prevent hanging
            "ssl": "on",  # FIXED: Enable SSL for security
            "connection_timeout": "20",  # FIXED: Reduced from 30
            "socket_timeout": "30",  # FIXED: Reduced from 60
            "login_timeout": "15",  # FIXED: Reduced from 30
            "network_timeout": "30",  # FIXED: Reduced from 60
            "query_timeout": "60",  # FIXED: Reduced from 120
            "statement_timeout_in_seconds": "60",  # FIXED: Reduced from 120
            # FIXED: Enhanced reliability improvements
            "retry_timeout": "10",  # FIXED: Reduced retry timeout
            "max_retry_count": "2",  # FIXED: Reduced retry attempts
            "retry_delay": "2",  # FIXED: Increased delay between retries
            # FIXED: Disable diagnostics to reduce overhead
            "enable_connection_diag": "false",
        }

        # Validate required Snowflake config
        required_config = ["sfUrl", "sfUser", "sfPassword"]
        missing = [k for k in required_config if not self.snowflake_options.get(k)]
        if missing:
            self.logger.warning(f"Missing Snowflake configuration: {missing}")
            self.snowflake_enabled = False
        else:
            self.snowflake_enabled = True
            self.logger.info(
                f"Snowflake configuration validated for URL: {snowflake_url}"
            )

    def define_schema(self) -> StructType:
        """Define the expected schema for sales CSV files"""
        return StructType(
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

    def read_csv_files(self, input_path: str) -> DataFrame:
        """Read CSV files from S3 with schema inference and error handling"""
        self.logger.info(f"Reading CSV files from: {input_path}")

        try:
            # Use schema inference for flexible CSV reading
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .option("dateFormat", "MM/dd/yyyy")
                .csv(input_path)
                .withColumn("source_file", input_file_name())
            )

            # Log the actual schema found
            self.logger.info(f"DEBUG: Inferred schema columns: {df.columns}")
            self.logger.info(f"DEBUG: Schema: {df.schema}")

            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from CSV files")

            if record_count == 0:
                self.logger.warning("No records found in input files")

            return df

        except Exception as e:
            self.logger.error(f"Failed to read CSV files: {str(e)}")
            raise

    def clean_and_transform(self, df: DataFrame, batch_id: str) -> DataFrame:
        """Clean and transform the raw sales data"""
        self.logger.info("Starting data cleaning and transformation")

        # Data cleaning transformations
        cleaned_df = (
            df
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
            # Parse dates properly (handle dd-MM-yyyy format found in data)
            .withColumn("Order Date", to_date(col("Order Date"), "dd-MM-yyyy"))
            .withColumn("Ship Date", to_date(col("Ship Date"), "dd-MM-yyyy"))
            # Handle numeric fields - cast to numeric and replace negatives and nulls
            .withColumn(
                "Sales",
                when(
                    col("Sales").isNull() | (col("Sales").cast("double") < 0), 0.0
                ).otherwise(col("Sales").cast("double")),
            )
            .withColumn(
                "Quantity",
                when(
                    col("Quantity").isNull() | (col("Quantity").cast("integer") <= 0), 1
                ).otherwise(col("Quantity").cast("integer")),
            )
            .withColumn(
                "Discount",
                when(
                    col("Discount").isNull() | (col("Discount").cast("double") < 0), 0.0
                )
                .when(
                    col("Discount").cast("double") > 1.0,
                    col("Discount").cast("double") / 100,
                )  # Convert percentage
                .otherwise(col("Discount").cast("double")),
            )
            .withColumn(
                "Profit", when(col("Profit").isNull(), 0.0).otherwise(col("Profit"))
            )
            # Calculate derived fields
            .withColumn(
                "unit_price",
                when(col("Quantity") > 0, col("Sales") / col("Quantity")).otherwise(
                    0.0
                ),
            )
            .withColumn(
                "profit_margin",
                when(col("Sales") > 0, col("Profit") / col("Sales")).otherwise(0.0),
            )
            # Add metadata fields
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("BATCH"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn(
                "partition_date", date_format(current_timestamp(), "yyyy-MM-dd")
            )
        )

        # Filter out invalid records
        valid_df = cleaned_df.filter(
            col("Order Date").isNotNull()
            & col("Sales").isNotNull()
            & (col("Sales") >= 0)
            & col("Quantity").isNotNull()
            & (col("Quantity") > 0)
        )

        initial_count = df.count()
        final_count = valid_df.count()
        filtered_count = initial_count - final_count

        self.logger.info(f"Data cleaning completed:")
        self.logger.info(f"  - Initial records: {initial_count}")
        self.logger.info(f"  - Valid records: {final_count}")
        self.logger.info(f"  - Filtered out: {filtered_count}")

        return valid_df

    def map_to_snowflake_schema(self, df: DataFrame) -> DataFrame:
        """Map DataFrame columns to Snowflake table schema"""
        return df.select(
            col("Order ID").alias("ORDER_ID"),
            col("Customer ID").alias("STORE_ID"),  # Using Customer ID as Store ID
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

    def test_snowflake_connection(self) -> bool:
        """Test Snowflake connection using direct connector (not Spark connector)"""
        if not self.snowflake_enabled:
            self.logger.info("Snowflake is disabled, skipping connection test")
            return False

        try:
            self.logger.info("Testing Snowflake connection...")

            # Use direct snowflake.connector instead of Spark connector to avoid hanging
            import snowflake.connector

            conn = snowflake.connector.connect(
                user=self.snowflake_options["sfUser"],
                password=self.snowflake_options["sfPassword"],
                account=f"{os.getenv('SNOWFLAKE_ORGANIZATION_NAME')}-{os.getenv('SNOWFLAKE_ACCOUNT_NAME')}",
                warehouse=self.snowflake_options["sfWarehouse"],
                database=self.snowflake_options["sfDatabase"],
                schema=self.snowflake_options["sfSchema"],
                login_timeout=15,
                network_timeout=30,
            )

            # Test simple query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            self.logger.info(
                f"SUCCESS: Snowflake connection successful (version: {result[0]})"
            )
            return True

        except Exception as e:
            self.logger.warning(f"ERROR: Snowflake connection test failed: {str(e)}")
            self.logger.warning("Pipeline will continue without Snowflake integration")
            return False

    def write_to_snowflake(
        self, df: DataFrame, table_name: str, mode: str = "append"
    ) -> bool:
        """Write DataFrame to Snowflake table with graceful fallback"""
        if not self.snowflake_enabled:
            self.logger.info("Snowflake is disabled, skipping write operation")
            return False

        # Check if DataFrame is empty (known issue with Snowflake Spark connector)
        record_count = df.count()
        if record_count == 0:
            self.logger.warning(
                "WARNING: DataFrame is empty (0 records). Skipping Snowflake write to avoid connector hang."
            )
            self.logger.info(
                "NOTE: This is a known issue with the Snowflake Spark connector when writing empty DataFrames."
            )
            return True  # Return True as this is expected behavior, not a failure

        self.logger.info(
            f"Writing {record_count} records to Snowflake table: {table_name}"
        )

        try:
            # PRODUCTION FIX: Repartition DataFrame for optimal performance
            # Snowflake recommends 10-100MB partitions for best performance
            optimal_partitions = max(
                1, record_count // 5000
            )  # ~5K records per partition
            if df.rdd.getNumPartitions() != optimal_partitions:
                self.logger.info(
                    f"Repartitioning from {df.rdd.getNumPartitions()} to {optimal_partitions} partitions"
                )
                df = df.repartition(optimal_partitions)

            # FIXED: Enhanced Snowflake options to prevent hanging
            write_options = self.snowflake_options.copy()
            write_options.update(
                {
                    # FIXED: Conservative settings for reliability (compatible with 2.11.3)
                    "partition_size_in_mb": "16",  # FIXED: Much smaller partitions
                    "parallel": str(
                        min(optimal_partitions, 4)
                    ),  # FIXED: Limit parallelism
                    "truncate_table": "off",  # Safer for production
                    "continue_on_error": "off",  # Fail fast on errors
                    "abort_detached_query": "on",  # FIXED: Enable to prevent hanging
                    "sfCompressionType": "gzip",  # FIXED: Explicit compression
                }
            )

            # Use optimized write configuration
            self.logger.info(
                f"Writing with {min(optimal_partitions, 4)} partitions, 16MB partition size"
            )
            (
                df.write.format("snowflake")
                .options(**write_options)
                .option("dbtable", table_name)
                .option("sfCompressionType", "gzip")
                .mode(mode)
                .save()
            )

            self.logger.info(
                f"SUCCESS: Successfully wrote {record_count} records to {table_name}"
            )
            return True

        except Exception as e:
            self.logger.error(f"ERROR: Failed to write to Snowflake: {str(e)}")
            self.logger.info("Pipeline will continue without Snowflake write")
            return False

    def run_etl(
        self, input_path: str, output_table: str, batch_id: Optional[str] = None
    ) -> Dict:
        """Run the complete ETL process"""
        if not batch_id:
            batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        self.logger.info(f"Starting ETL job with batch_id: {batch_id}")

        start_time = datetime.now()

        try:
            # Step 1: Read CSV files
            self.logger.info("DEBUG: Starting Step 1 - Reading CSV files")
            raw_df = self.read_csv_files(input_path)
            self.logger.info("DEBUG: Step 1 completed - CSV files read successfully")

            # Step 2: Clean and transform
            self.logger.info(
                "DEBUG: Starting Step 2 - Data cleaning and transformation"
            )
            cleaned_df = self.clean_and_transform(raw_df, batch_id)
            self.logger.info("DEBUG: Step 2 completed - Data cleaning finished")

            # Step 3: Map to Snowflake schema
            self.logger.info("DEBUG: Starting Step 3 - Mapping to Snowflake schema")
            final_df = self.map_to_snowflake_schema(cleaned_df)
            self.logger.info("DEBUG: Step 3 completed - Schema mapping finished")

            # Step 4: Test Snowflake connection (if enabled)
            snowflake_connection_ok = False
            if self.snowflake_enabled:
                self.logger.info(
                    "DEBUG: Starting Step 4a - Testing Snowflake connection"
                )
                snowflake_connection_ok = self.test_snowflake_connection()
                if snowflake_connection_ok:
                    self.logger.info(
                        "DEBUG: Step 4a completed - Snowflake connection verified"
                    )
                else:
                    self.logger.warning(
                        "DEBUG: Step 4a completed - Snowflake connection failed, continuing without Snowflake"
                    )

            # Step 5: Write to Snowflake (if connection is OK)
            snowflake_write_success = False
            if snowflake_connection_ok:
                self.logger.info("DEBUG: Starting Step 5 - Writing to Snowflake")
                snowflake_write_success = self.write_to_snowflake(
                    final_df, output_table
                )
                if snowflake_write_success:
                    self.logger.info(
                        "DEBUG: Step 5 completed - Snowflake write successful"
                    )
                else:
                    self.logger.warning(
                        "DEBUG: Step 5 completed - Snowflake write failed"
                    )
            else:
                self.logger.info(
                    "DEBUG: Skipping Step 5 - Snowflake write (connection unavailable)"
                )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "batch_id": batch_id,
                "records_processed": final_df.count(),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "snowflake_enabled": self.snowflake_enabled,
                "snowflake_connection_ok": snowflake_connection_ok,
                "snowflake_write_success": snowflake_write_success,
            }

            self.logger.info(f"ETL job completed successfully: {result}")
            return result

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "failed",
                "batch_id": batch_id,
                "error": str(e),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }

            self.logger.error(f"ETL job failed: {result}")
            raise


def create_spark_session(app_name: str = "SalesETL") -> SparkSession:
    """Create and configure Spark session with required packages and AWS credentials"""
    # Get AWS credentials from environment variables
    # Compatible with both local execution and Databricks full edition
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config(
            "spark.network.timeout", "600s"
        )  # FIXED: Reduced timeout for faster failure detection
        .config(
            "spark.sql.execution.arrow.pyspark.enabled", "false"
        )  # FIXED: Disable Arrow (conflicts with Snowflake)
        .config(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
        )  # FIXED: Enhanced serialization
        .config("spark.kryo.registrationRequired", "false")
        .config(
            "spark.driver.extraJavaOptions",
            "-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp",
        )
        .config(
            "spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.19.0,"
            "net.snowflake:spark-snowflake_2.12:3.1.4,"  # FIXED: Using latest stable version compatible with Spark 3.5.0
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367",
        )
        .getOrCreate()
    )


def main():
    """Main entry point for the ETL job"""
    parser = argparse.ArgumentParser(description="Sales Data ETL Job")
    parser.add_argument(
        "--input-path",
        required=True,
        help="S3 path to input CSV files (e.g., s3a://bucket/path/)",
    )
    parser.add_argument(
        "--output-table",
        default="SALES_BATCH_RAW",
        help="Snowflake table name for output",
    )
    parser.add_argument("--batch-id", help="Custom batch ID (default: auto-generated)")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Create ETL job instance
        etl_job = SalesETLJob(spark)

        # Run ETL process
        result = etl_job.run_etl(args.input_path, args.output_table, args.batch_id)

        logger.info("ETL job completed successfully")
        print(f"ETL Result: {result}")

        return 0

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
