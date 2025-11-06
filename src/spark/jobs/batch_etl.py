#!/usr/bin/env python3
"""
PySpark ETL Job for Sales Data Processing

Processes sales data from multiple sources with automatic schema normalization:
- CSV files (batch processing)
- JSON files (streaming via Kafka Connect)

Schema Normalization:
All column names are normalized to snake_case immediately after reading from
Bronze layer. This ensures consistent processing regardless of source format
or column naming convention.

Medallion Architecture Flow:
Bronze S3 → Spark ETL (normalization + transformation) → Delta Lake Silver → Snowflake Gold

Usage:
    python batch_etl.py --input-path s3://bucket/path/ --batch-id streaming-202510020046
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
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
    regexp_extract,
    regexp_replace,
    row_number,
    sha2,
    concat_ws,
    coalesce,
)
from pyspark.sql.window import Window
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
        """Define the expected schema for sales CSV files (24 fields)"""
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
                StructField("Market", StringType(), True),  # ADDED
                StructField("Region", StringType(), True),
                StructField("Product ID", StringType(), True),
                StructField("Category", StringType(), True),
                StructField("Sub-Category", StringType(), True),
                StructField("Product Name", StringType(), True),
                StructField("Sales", DoubleType(), True),
                StructField("Quantity", IntegerType(), True),
                StructField("Discount", DoubleType(), True),
                StructField("Profit", DoubleType(), True),
                StructField("Shipping Cost", DoubleType(), True),  # ADDED
                StructField("Order Priority", StringType(), True),  # ADDED
            ]
        )

    def normalize_column_names(self, df: DataFrame) -> DataFrame:
        """
        Normalize column names to snake_case for consistent data processing.

        This method handles multiple naming conventions from different data sources:
        - CSV batch files (Title Case with spaces)
        - JSON streaming data (snake_case)
        - Future integrations (CamelCase, kebab-case, etc.)

        Transformation Rules:
        1. CamelCase split: OrderID → Order_ID
        2. Replace separators: spaces/dashes/dots → underscores
        3. Remove special characters
        4. Convert to lowercase
        5. Clean up multiple/leading/trailing underscores

        Examples:
            "Order ID" → "order_id" (CSV batch)
            "order_id" → "order_id" (JSON streaming, unchanged)
            "OrderID" → "order_id" (CamelCase)
            "order-id" → "order_id" (kebab-case)
            "Order@ID#123" → "order_id_123" (special chars)

        Args:
            df: Input DataFrame with any column naming convention

        Returns:
            DataFrame with all columns normalized to snake_case

        Note:
            This normalization happens immediately after reading Bronze data,
            ensuring all downstream transformations work consistently regardless
            of the source data format (CSV vs JSON).
        """
        import re

        for old_col in df.columns:
            # Step 1: Handle CamelCase - insert underscore before capital letters
            new_col = re.sub("([a-z0-9])([A-Z])", r"\1_\2", old_col)

            # Step 2: Replace spaces, dashes, and dots with underscores
            new_col = re.sub(r"[\s\-\.]+", "_", new_col)

            # Step 3: Remove non-alphanumeric characters except underscores
            new_col = re.sub(r"[^a-zA-Z0-9_]+", "_", new_col)

            # Step 4: Convert to lowercase
            new_col = new_col.lower()

            # Step 5: Remove leading/trailing underscores and collapse multiple underscores
            new_col = re.sub(r"_+", "_", new_col).strip("_")

            # Step 6: Rename if changed
            if old_col != new_col:
                self.logger.info(f"Column normalized: '{old_col}' → '{new_col}'")
                df = df.withColumnRenamed(old_col, new_col)

        return df

    def read_bronze_data(
        self, bucket_name: str, batch_date: str, input_path_override: str = None
    ) -> DataFrame:
        """Read data from Bronze layer S3 bucket with Medallion architecture

        Supports both CSV (batch) and JSON (streaming from Kafka Connect) formats.
        """
        # Bronze layer path pattern: sales_data/year=YYYY/month=MM/day=DD/
        year = batch_date[:4]
        month = batch_date[5:7]
        day = batch_date[8:10]

        # Use override path if provided (for streaming with custom paths)
        if input_path_override:
            bronze_path = input_path_override
        else:
            bronze_path = (
                f"s3a://{bucket_name}/sales_data/year={year}/month={month}/day={day}/"
            )

        self.logger.info(f"Reading Bronze data from: {bronze_path}")
        self.logger.info(
            f"Batch date: {batch_date} (Year: {year}, Month: {month}, Day: {day})"
        )

        # Detect if this is streaming data (JSON from Kafka Connect) or batch data (CSV)
        is_streaming = "sales_events" in bronze_path or input_path_override

        try:
            if is_streaming:
                # Read JSON files from Kafka Connect (streaming data)
                self.logger.info("Detected streaming data - reading JSON format")
                df = (
                    self.spark.read.option("multiLine", "false")
                    .json(bronze_path + "*.json")
                    .withColumn(
                        "source_file", regexp_extract(input_file_name(), r"([^/]+)$", 1)
                    )
                    .withColumn("bronze_ingestion_timestamp", current_timestamp())
                    .withColumn("batch_id", lit(batch_date))
                )
            else:
                # Read CSV files from Bronze layer (batch data)
                # IMPORTANT: inferSchema=false to preserve postal codes as STRING
                # Bug fix: inferSchema=true treats "08701" as 8701.0, losing leading zeros
                self.logger.info(
                    "Detected batch data - reading CSV format (all STRING)"
                )
                df = (
                    self.spark.read.option("header", "true")
                    .option("inferSchema", "false")  # Read all columns as STRING
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                    .option("dateFormat", "dd-MM-yyyy")  # Updated for Bronze CSV format
                    .csv(bronze_path + "*.csv")
                    .withColumn(
                        "source_file", regexp_extract(input_file_name(), r"([^/]+)$", 1)
                    )
                    .withColumn("bronze_ingestion_timestamp", current_timestamp())
                    .withColumn("batch_id", lit(batch_date))
                )

            # Log Bronze layer metadata
            self.logger.info(f"Bronze schema columns: {df.columns}")
            self.logger.info(f"Bronze schema: {df.schema}")

            record_count = df.count()
            self.logger.info(
                f"Successfully read {record_count} records from Bronze layer"
            )

            # Normalize column names for consistency across CSV and JSON sources
            self.logger.info("Normalizing column names to snake_case...")
            df = self.normalize_column_names(df)
            self.logger.info(f"Normalized schema: {df.columns}")

            if record_count == 0:
                self.logger.warning("No records found in Bronze layer for this batch")
                # Fall back to reading from root bucket (existing CSV files) for backward compatibility
                self.logger.info("Attempting fallback to root bucket CSV files...")
                return self.read_csv_files_fallback(f"s3a://{bucket_name}/")

            return df

        except Exception as e:
            self.logger.error(f"Failed to read Bronze layer data: {str(e)}")
            self.logger.info("Attempting fallback to direct CSV reading...")
            return self.read_csv_files_fallback(f"s3a://{bucket_name}/")

    def read_csv_files_fallback(self, input_path: str) -> DataFrame:
        """Fallback method: Read CSV files directly (backward compatibility)"""
        self.logger.info(f"Fallback: Reading CSV files from: {input_path}")

        try:
            # Read all columns as STRING to preserve postal codes
            # Bug fix: inferSchema=true treats "08701" as 8701.0, losing leading zeros
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "false")  # Read all columns as STRING
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .option("dateFormat", "MM/dd/yyyy")
                .csv(input_path + "*.csv")
                .withColumn(
                    "source_file", regexp_extract(input_file_name(), r"([^/]+)$", 1)
                )
                .withColumn("bronze_ingestion_timestamp", current_timestamp())
            )

            # Log the actual schema found
            self.logger.info(f"Fallback schema columns: {df.columns}")
            self.logger.info(f"Fallback schema: {df.schema}")

            record_count = df.count()
            self.logger.info(
                f"Fallback: Successfully read {record_count} records from CSV files"
            )

            if record_count == 0:
                self.logger.warning("No records found in fallback CSV files")

            return df

        except Exception as e:
            self.logger.error(f"Fallback CSV reading failed: {str(e)}")
            raise

    def clean_and_transform(self, df: DataFrame, batch_id: str) -> DataFrame:
        """
        Clean and transform the raw sales data.

        NOTE: This method expects snake_case column names from normalize_column_names().
        All column references use snake_case (order_id, customer_id, etc.) to work
        consistently with both CSV batch data and JSON streaming data.
        """
        self.logger.info("Starting data cleaning and transformation")

        # Get SALES_THRESHOLD from environment
        SALES_THRESHOLD = float(os.getenv("SALES_THRESHOLD", "10000"))
        self.logger.info(f"Using SALES_THRESHOLD: ${SALES_THRESHOLD:,.2f}")

        # Preserve upstream identifiers so Silver keeps traceability
        if "row_id" in df.columns:
            df = df.withColumnRenamed("row_id", "source_row_id")
        else:
            df = df.withColumn("source_row_id", lit(None).cast("string"))

        # Data cleaning transformations
        cleaned_df = (
            df
            # Remove rows with null order IDs
            .filter(col("order_id").isNotNull())
            # Clean and standardize text fields
            .withColumn("order_id", trim(col("order_id")))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("product_id", trim(col("product_id")))
            .withColumn("source_row_id", trim(col("source_row_id")))
            .withColumn(
                "source_row_id",
                when(
                    col("source_row_id").isNull() | (col("source_row_id") == ""),
                    lit(None).cast("string"),
                ).otherwise(col("source_row_id")),
            )
            # Standardize categorical fields: normalize whitespace and convert to uppercase for consistency
            .withColumn(
                "category", upper(regexp_replace(trim(col("category")), r"\s+", " "))
            )
            .withColumn(
                "sub_category",
                upper(regexp_replace(trim(col("sub_category")), r"\s+", " ")),
            )
            .withColumn("segment", regexp_replace(trim(col("segment")), r"\s+", " "))
            # Standardize geographic fields: preserve original case, normalize whitespace
            .withColumn("region", regexp_replace(trim(col("region")), r"\s+", " "))
            .withColumn("country", regexp_replace(trim(col("country")), r"\s+", " "))
            .withColumn("state", regexp_replace(trim(col("state")), r"\s+", " "))
            .withColumn("city", regexp_replace(trim(col("city")), r"\s+", " "))
            # Parse dates properly (handle dd-MM-yyyy format found in data)
            .withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))
            .withColumn("ship_date", to_date(col("ship_date"), "dd-MM-yyyy"))
            # Cast numeric fields to proper types (now that all columns are STRING from inferSchema=false)
            .withColumn("sales", col("sales").cast("double"))
            .withColumn("quantity", col("quantity").cast("integer"))
            .withColumn("discount", col("discount").cast("double"))
            .withColumn("profit", col("profit").cast("double"))
            .withColumn("shipping_cost", col("shipping_cost").cast("double"))
            # Apply SALES_THRESHOLD cap
            .withColumn(
                "sales",
                when(col("sales") > SALES_THRESHOLD, SALES_THRESHOLD).otherwise(
                    col("sales")
                ),
            )
            # Calculate derived fields
            .withColumn(
                "unit_price",
                when(col("quantity") > 0, col("sales") / col("quantity")).otherwise(
                    0.0
                ),
            )
            .withColumn(
                "profit_margin",
                when(col("sales") > 0, col("profit") / col("sales")).otherwise(0.0),
            )
            # Add metadata fields
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn(
                "processing_timestamp", current_timestamp()
            )  # For MERGE deduplication
            .withColumn("source_system", lit("BATCH"))
            .withColumn("batch_id", lit(batch_id))
            .withColumn(
                "partition_date", date_format(current_timestamp(), "yyyy-MM-dd")
            )
        )

        cleaned_df = cleaned_df.withColumn(
            "row_id",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("source_row_id"), lit("")),
                    coalesce(col("order_id"), lit("")),
                    coalesce(col("product_id"), lit("")),
                    coalesce(col("source_system"), lit("")),
                ),
                256,
            ),
        )

        initial_count = cleaned_df.count()
        self.logger.info(f"Cleaned {initial_count} records (SALES_THRESHOLD applied)")

        return cleaned_df

    def validate_geography(self, df: DataFrame) -> DataFrame:
        """
        Validate geographic data consistency and flag suspicious records.

        Industry best practice: Silver layer should validate referential integrity.
        This method adds a flag column to identify records with known invalid
        city/country combinations without rejecting them outright.

        Known Issues from Research:
        - São Paulo appearing in Chile (should be Brazil)
        - Buenos Aires in Chile (should be Argentina)
        - Lagos in South Africa (should be Nigeria)
        - Cairo in Nigeria (city exists in both, but context dependent)

        Returns:
            DataFrame with geo_quality_flag column added
        """
        self.logger.info("Validating geographic data consistency...")

        # Known invalid city/country combinations (from our data quality analysis)
        invalid_geo_combinations = [
            ("SÃO PAULO", "CHILE"),  # Should be Brazil
            ("BUENOS AIRES", "CHILE"),  # Should be Argentina
            ("LAGOS", "SOUTH AFRICA"),  # Should be Nigeria
            # Add more known invalid combinations as discovered
        ]

        # Build condition for flagging invalid combinations
        geo_condition = lit(False)
        for city, country in invalid_geo_combinations:
            geo_condition = geo_condition | (
                (upper(trim(col("city"))) == city)
                & (upper(trim(col("country"))) == country)
            )

        # Add geographic quality flag
        df_with_flag = df.withColumn(
            "geo_quality_flag",
            when(geo_condition, "INVALID_CITY_COUNTRY").otherwise("VALID"),
        )

        # Log geographic quality metrics
        invalid_count = df_with_flag.filter(col("geo_quality_flag") != "VALID").count()
        if invalid_count > 0:
            self.logger.warning(
                f"Geographic validation: {invalid_count} records with suspicious city/country combinations"
            )
            self.logger.warning(
                "These records are flagged but NOT rejected - manual review recommended"
            )
        else:
            self.logger.info(
                "Geographic validation: All city/country combinations appear valid"
            )

        return df_with_flag

    def validate_and_quarantine(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply data quality validation rules and split valid/rejected records.

        Validation Rules:
        1. sales > 0
        2. quantity > 0
        3. order_date <= ship_date (logical date sequence)
        4. No duplicates on (order_id, product_id)

        Returns:
            Tuple of (valid_df, rejected_df)
        """
        self.logger.info("Starting data quality validation")

        # Add rejection reasons column
        df = df.withColumn("rejection_reason", lit(None).cast("string"))

        # Identify invalid records with reasons
        # IMPORTANT: Check business keys first - records without order_id/product_id cannot be merged
        df = df.withColumn(
            "rejection_reason",
            when(
                col("order_id").isNull() | (trim(col("order_id")) == ""),
                "missing_order_id",
            )
            .when(
                col("product_id").isNull() | (trim(col("product_id")) == ""),
                "missing_product_id",
            )
            .when(col("sales").isNull() | (col("sales") <= 0), "invalid_sales")
            .when(col("quantity").isNull() | (col("quantity") <= 0), "invalid_quantity")
            .when(
                col("order_date").isNull() | col("ship_date").isNull(), "missing_date"
            )
            .when(col("order_date") > col("ship_date"), "date_violation")
            .otherwise(None),
        )

        # Split into valid and rejected
        valid_df = df.filter(col("rejection_reason").isNull()).drop("rejection_reason")
        rejected_df = df.filter(col("rejection_reason").isNotNull())

        # STEP 1: Deduplicate by source_row_id first (handles re-uploads of same source files)
        # If same CSV uploaded multiple times, upstream row_ids will be identical
        # Keep record with latest processing_timestamp (most recent upload)
        valid_count_initial = valid_df.count()

        window_source = Window.partitionBy(
            coalesce(col("source_row_id"), col("row_id"))
        ).orderBy(col("processing_timestamp").desc())
        dedup_stage = valid_df.withColumn("_row_rank", row_number().over(window_source))
        valid_df = dedup_stage.filter(col("_row_rank") == 1).drop("_row_rank")

        source_duplicates_removed = valid_count_initial - valid_df.count()

        # STEP 2: Deduplicate on business key (order_id, product_id)
        # Handles true business duplicates (different row_ids, same transaction)
        valid_count_before = valid_df.count()
        valid_df = valid_df.dropDuplicates(["order_id", "product_id"])
        valid_count_after = valid_df.count()
        business_key_duplicates_removed = valid_count_before - valid_count_after

        duplicates_removed = source_duplicates_removed + business_key_duplicates_removed

        # Fail fast if duplicates still exist after deduplication
        residual_duplicates = (
            valid_df.groupBy("row_id").count().filter(col("count") > 1).count()
        )
        if residual_duplicates > 0:
            self.logger.error(
                f"Detected {residual_duplicates} duplicate row_id values after deduplication."
            )
            raise ValueError(
                "Duplicate row_id values remain after deduplication; aborting Silver write."
            )

        # Log validation results
        rejected_count = rejected_df.count()
        total_records = valid_count_initial + rejected_count

        self.logger.info("=" * 80)
        self.logger.info("DATA QUALITY VALIDATION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total records processed: {total_records:,}")
        self.logger.info(
            f"  VALID records (going to Silver): {valid_count_after:,} ({valid_count_after/total_records*100:.1f}%)"
        )
        self.logger.info(
            f"  REJECTED records (going to Quarantine): {rejected_count:,} ({rejected_count/total_records*100:.1f}%)"
        )
        self.logger.info(
            f"  DUPLICATES removed (source_row_id): {source_duplicates_removed:,} (re-uploaded data)"
        )
        self.logger.info(
            f"  DUPLICATES removed (business key): {business_key_duplicates_removed:,} (true duplicates)"
        )
        self.logger.info(f"  TOTAL DUPLICATES removed: {duplicates_removed:,}")
        self.logger.info("=" * 80)

        if rejected_count > 0:
            self.logger.info("REJECTION BREAKDOWN:")
            rejection_breakdown = (
                rejected_df.groupBy("rejection_reason").count().collect()
            )
            for row in rejection_breakdown:
                self.logger.info(
                    f"  - {row['rejection_reason']}: {row['count']:,} records"
                )
            self.logger.info("=" * 80)

        return valid_df, rejected_df

    def write_to_quarantine(self, rejected_df: DataFrame, output_path: str) -> bool:
        """
        Write rejected records to quarantine bucket in Delta format.

        Args:
            rejected_df: DataFrame with rejected records and rejection_reason column
            output_path: S3 path for quarantine (e.g., s3://bucket/quarantine/)

        Returns:
            True if successful, False otherwise
        """
        rejected_count = rejected_df.count()

        if rejected_count == 0:
            self.logger.info("No rejected records to quarantine")
            return True

        self.logger.info(
            f"Writing {rejected_count} rejected records to quarantine: {output_path}"
        )

        try:
            # Write to Delta Lake with append mode (accumulate all rejected records)
            (
                rejected_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(output_path)
            )

            self.logger.info("=" * 80)
            self.logger.info("QUARANTINE WRITE SUCCESSFUL")
            self.logger.info(f"  Location: {output_path}")
            self.logger.info(f"  Records written: {rejected_count:,}")
            self.logger.info(f"  Format: Delta Lake (append mode)")
            self.logger.info("=" * 80)
            return True

        except Exception as e:
            self.logger.error(f"Failed to write to quarantine: {e}")
            import traceback

            self.logger.error(traceback.format_exc())
            return False

    def map_to_snowflake_schema(self, df: DataFrame) -> DataFrame:
        """
        Map DataFrame columns to Snowflake table schema.

        Source columns: snake_case (normalized)
        Target columns: UPPER_CASE (Snowflake convention)
        """
        return df.select(
            col("order_id").alias("ORDER_ID"),
            col("customer_id").alias("STORE_ID"),  # Using customer_id as Store ID
            col("product_id").alias("PRODUCT_ID"),
            col("product_name").alias("PRODUCT_NAME"),
            col("category").alias("CATEGORY"),
            col("sub_category").alias("SUB_CATEGORY"),
            col("quantity").alias("QUANTITY"),
            col("unit_price").alias("UNIT_PRICE"),
            col("sales").alias("TOTAL_PRICE"),
            col("order_date").alias("ORDER_DATE"),
            col("ship_date").alias("SHIP_DATE"),
            col("sales").alias("SALES"),
            col("profit").alias("PROFIT"),
            col("shipping_cost").alias("SHIPPING_COST"),
            col("segment").alias("CUSTOMER_SEGMENT"),
            col("market").alias("MARKET"),
            col("region").alias("REGION"),
            col("country").alias("COUNTRY"),
            col("state").alias("STATE"),
            col("city").alias("CITY"),
            col("order_priority").alias("ORDER_PRIORITY"),
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

    def write_to_delta_silver(
        self, df: DataFrame, output_path: str, mode: str = "overwrite"
    ) -> bool:
        """
        Write DataFrame to Delta Lake Silver layer using MERGE for unified batch+streaming.

        This method implements Delta Lake MERGE (upsert) pattern to handle:
        - Batch and streaming data converging into single Silver table
        - Deduplication based on order_id business key
        - Late-arriving data updates
        - Concurrent writes with ACID guarantees

        Args:
            df: DataFrame to write
            output_path: S3 path for unified Delta Lake (e.g., s3://bucket/silver/sales/)
            mode: Deprecated - MERGE logic determines behavior automatically

        Returns:
            True if successful, False otherwise
        """
        record_count = df.count()

        if record_count == 0:
            self.logger.warning("No records to write to Silver Delta Lake")
            return True

        self.logger.info("=" * 80)
        self.logger.info("WRITING TO SILVER LAYER")
        self.logger.info("=" * 80)
        self.logger.info(f"  Target location: {output_path}")
        self.logger.info(f"  Records to write: {record_count:,}")
        self.logger.info(f"  Format: Delta Lake")

        try:
            # Check if Delta table exists
            if DeltaTable.isDeltaTable(self.spark, output_path):
                self.logger.info("Delta table exists - using MERGE for upsert")

                # Load existing Delta table
                deltaTable = DeltaTable.forPath(self.spark, output_path)

                target_columns = set(deltaTable.toDF().columns)
                if "source_row_id" not in target_columns:
                    self.logger.warning(
                        "Existing Delta table is missing source_row_id; aligning schema before merge."
                    )
                    (
                        deltaTable.toDF()
                        .withColumn("source_row_id", lit(None).cast("string"))
                        .write.format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", "true")
                        .save(output_path)
                    )
                    deltaTable = DeltaTable.forPath(self.spark, output_path)

                # MERGE: Upsert based on composite business key (order_id, product_id)
                # One order can have multiple products, so we need both keys for correct matching
                # This handles batch/streaming convergence and deduplication
                (
                    deltaTable.alias("target")
                    .merge(
                        df.alias("source"),
                        "target.order_id = source.order_id AND target.product_id = source.product_id",  # Composite business key
                    )
                    .whenMatchedUpdate(
                        # Update existing record only if source is newer
                        condition="source.processing_timestamp > target.processing_timestamp",
                        set={
                            "row_id": "source.row_id",
                            "source_row_id": "source.source_row_id",
                            "customer_id": "source.customer_id",
                            "product_id": "source.product_id",
                            "product_name": "source.product_name",
                            "category": "source.category",
                            "sub_category": "source.sub_category",
                            "quantity": "source.quantity",
                            "sales": "source.sales",
                            "profit": "source.profit",
                            "discount": "source.discount",
                            "shipping_cost": "source.shipping_cost",
                            "order_date": "source.order_date",
                            "ship_date": "source.ship_date",
                            "ship_mode": "source.ship_mode",
                            "segment": "source.segment",
                            "market": "source.market",
                            "region": "source.region",
                            "country": "source.country",
                            "state": "source.state",
                            "city": "source.city",
                            "postal_code": "source.postal_code",
                            "order_priority": "source.order_priority",
                            "unit_price": "source.unit_price",
                            "profit_margin": "source.profit_margin",
                            "source_system": "source.source_system",
                            "batch_id": "source.batch_id",
                            "processing_timestamp": "source.processing_timestamp",
                            "partition_date": "source.partition_date",
                        },
                    )
                    .whenNotMatchedInsertAll()  # Insert new records
                    .execute()
                )

                self.logger.info(
                    f"SUCCESS: Merged {record_count} records into Delta Silver layer"
                )

            else:
                # First write: Create table with overwrite
                self.logger.info("Creating new Delta table with initial data")

                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .option("delta.columnMapping.mode", "name")
                    .save(output_path)
                )

                self.logger.info(
                    f"SUCCESS: Created Delta table with {record_count} records"
                )

            self.logger.info(f"Location: {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write to Delta Silver: {e}")
            import traceback

            self.logger.error(traceback.format_exc())
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
            # Step 1: Read data from Bronze layer (Medallion architecture)
            self.logger.info("DEBUG: Starting Step 1 - Reading data from Bronze layer")

            # Parse input_path to extract bucket name and batch date
            if input_path.startswith("s3://") or input_path.startswith("s3a://"):
                # Extract bucket name from S3 path: s3://bucket-name/
                bucket_name = input_path.split("/")[2]
            else:
                # Default bucket name if not provided
                bucket_name = os.getenv("RAW_BUCKET", "raw-sales-pipeline-976404003846")

            # Use batch_id as date if it follows YYYY-MM-DD format, otherwise use today
            if (
                batch_id
                and len(batch_id) >= 10
                and batch_id[4] == "-"
                and batch_id[7] == "-"
            ):
                batch_date = batch_id[:10]  # Extract YYYY-MM-DD
            else:
                batch_date = datetime.now().strftime("%Y-%m-%d")

            self.logger.info(
                f"Bronze layer parameters: bucket={bucket_name}, batch_date={batch_date}"
            )

            # Read from Bronze layer with fallback to direct CSV
            # Pass input_path for streaming data (contains custom path with sales_events)
            input_path_override = (
                input_path.replace("s3://", "s3a://")
                if "sales_events" in input_path
                else None
            )
            raw_df = self.read_bronze_data(bucket_name, batch_date, input_path_override)
            self.logger.info("DEBUG: Step 1 completed - Bronze data read successfully")

            # Step 2: Clean and transform
            self.logger.info(
                "DEBUG: Starting Step 2 - Data cleaning and transformation"
            )
            cleaned_df = self.clean_and_transform(raw_df, batch_id)
            self.logger.info("DEBUG: Step 2 completed - Data cleaning finished")

            # Step 2.5: Geographic validation
            self.logger.info("DEBUG: Starting Step 2.5 - Geographic validation")
            validated_geo_df = self.validate_geography(cleaned_df)
            self.logger.info(
                "DEBUG: Step 2.5 completed - Geographic validation finished"
            )

            # Step 3: Validate and quarantine
            self.logger.info("DEBUG: Starting Step 3 - Data quality validation")
            valid_df, rejected_df = self.validate_and_quarantine(validated_geo_df)
            self.logger.info("DEBUG: Step 3 completed - Validation finished")

            # Step 4: Write rejected records to quarantine
            self.logger.info("DEBUG: Starting Step 4 - Writing to quarantine")
            processed_bucket = os.getenv(
                "PROCESSED_BUCKET", "processed-sales-pipeline-976404003846"
            )
            quarantine_path = f"s3a://{processed_bucket}/quarantine/"

            quarantine_success = self.write_to_quarantine(rejected_df, quarantine_path)

            if quarantine_success:
                self.logger.info(
                    "DEBUG: Step 4 completed - Quarantine write successful"
                )
            else:
                self.logger.error("DEBUG: Step 4 failed - Quarantine write failed")

            # Step 5: Write to Delta Lake Silver layer
            self.logger.info("DEBUG: Starting Step 5 - Writing to Delta Silver")
            silver_path = f"s3a://{processed_bucket}/silver/sales/"

            delta_write_success = self.write_to_delta_silver(
                valid_df, silver_path, mode="overwrite"
            )

            if delta_write_success:
                self.logger.info(
                    "DEBUG: Step 5 completed - Delta Silver write successful"
                )
            else:
                self.logger.error("DEBUG: Step 5 failed - Delta Silver write failed")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "batch_id": batch_id,
                "records_processed": valid_df.count(),
                "records_rejected": rejected_df.count(),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "delta_write_success": delta_write_success,
                "quarantine_write_success": quarantine_success,
                "silver_path": silver_path if delta_write_success else None,
                "quarantine_path": quarantine_path if quarantine_success else None,
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
            "com.amazonaws:aws-java-sdk-bundle:1.12.367,"
            "io.delta:delta-spark_2.12:3.2.0",  # Delta Lake 3.2.0 for Spark 3.5.x ACID transactions
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
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
