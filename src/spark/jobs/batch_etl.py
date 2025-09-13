#!/usr/bin/env python3
"""
PySpark ETL Job for Sales Data Batch Processing

This job processes CSV files from S3, transforms the data, and loads it into Snowflake.
Designed to run on Databricks or standalone Spark clusters.

Usage:
    spark-submit --packages net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.11.3 \
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

        # Snowflake connection properties
        self.snowflake_options = {
            "sfUrl": os.getenv("SNOWFLAKE_ACCOUNT"),
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
            "sfSchema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        }

        # Validate required Snowflake config
        required_config = ["sfUrl", "sfUser", "sfPassword"]
        missing = [k for k in required_config if not self.snowflake_options.get(k)]
        if missing:
            raise ValueError(f"Missing required Snowflake configuration: {missing}")

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
        """Read CSV files from S3 with proper schema and error handling"""
        self.logger.info(f"Reading CSV files from: {input_path}")

        try:
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "false")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .option("dateFormat", "MM/dd/yyyy")
                .schema(self.define_schema())
                .csv(input_path)
                .withColumn("source_file", input_file_name())
            )

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
            # Parse dates properly
            .withColumn("Order Date", to_date(col("Order Date"), "MM/dd/yyyy"))
            .withColumn("Ship Date", to_date(col("Ship Date"), "MM/dd/yyyy"))
            # Handle numeric fields - replace negatives and nulls
            .withColumn(
                "Sales",
                when(col("Sales").isNull() | (col("Sales") < 0), 0.0).otherwise(
                    col("Sales")
                ),
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
                .when(
                    col("Discount") > 1.0, col("Discount") / 100
                )  # Convert percentage
                .otherwise(col("Discount")),
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

    def write_to_snowflake(
        self, df: DataFrame, table_name: str, mode: str = "append"
    ) -> None:
        """Write DataFrame to Snowflake table"""
        self.logger.info(
            f"Writing {df.count()} records to Snowflake table: {table_name}"
        )

        try:
            (
                df.write.format("snowflake")
                .options(**self.snowflake_options)
                .option("dbtable", table_name)
                .mode(mode)
                .save()
            )

            self.logger.info(f"Successfully wrote data to {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to write to Snowflake: {str(e)}")
            raise

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
            raw_df = self.read_csv_files(input_path)

            # Step 2: Clean and transform
            cleaned_df = self.clean_and_transform(raw_df, batch_id)

            # Step 3: Map to Snowflake schema
            final_df = self.map_to_snowflake_schema(cleaned_df)

            # Step 4: Write to Snowflake
            self.write_to_snowflake(final_df, output_table)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "batch_id": batch_id,
                "records_processed": final_df.count(),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
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
    """Create and configure Spark session with required packages"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config(
            "spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.14.3,"
            "net.snowflake:spark-snowflake_2.12:2.11.3",
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
