"""
Comprehensive unit tests for SalesETLJob class.

This module contains unit tests for all methods in the SalesETLJob class,
including data reading, transformation, schema mapping, and error handling.
"""

import pytest
import os
import tempfile
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

# PySpark imports with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        IntegerType,
    )

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

# Local imports
import sys

sys.path.append(os.path.dirname(__file__))

if SPARK_AVAILABLE:
    from src.spark.jobs.batch_etl import SalesETLJob, create_spark_session
    from tests.conftest_spark import create_spark_dataframe, assert_dataframe_equal


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSalesETLJob:
    """Test suite for SalesETLJob class."""

    def test_init_with_valid_config(self, spark_session: SparkSession):
        """Test SalesETLJob initialization with valid Snowflake configuration."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.snowflake.com",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            job = SalesETLJob(spark_session)

            assert job.spark == spark_session
            assert job.snowflake_options["sfUrl"] == "test.snowflake.com"
            assert job.snowflake_options["sfUser"] == "test_user"
            assert job.snowflake_options["sfPassword"] == "test_password"
            assert job.snowflake_options["sfDatabase"] == "SALES_DW"  # Default value

    def test_init_missing_required_config(self, spark_session: SparkSession):
        """Test SalesETLJob initialization fails with missing required configuration."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing required Snowflake configuration"
            ):
                SalesETLJob(spark_session)

    def test_define_schema(self, etl_job_with_mock_snowflake):
        """Test that define_schema returns the expected schema structure."""
        schema = etl_job_with_mock_snowflake.define_schema()

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]

        # Check that all required fields are present
        required_fields = [
            "Row ID",
            "Order ID",
            "Order Date",
            "Ship Date",
            "Ship Mode",
            "Customer ID",
            "Customer Name",
            "Segment",
            "Country",
            "City",
            "State",
            "Postal Code",
            "Region",
            "Product ID",
            "Category",
            "Sub-Category",
            "Product Name",
            "Sales",
            "Quantity",
            "Discount",
            "Profit",
        ]

        for field in required_fields:
            assert field in field_names, f"Missing required field: {field}"

        # Check data types for key fields
        field_dict = {field.name: field.dataType for field in schema.fields}
        assert isinstance(field_dict["Row ID"], IntegerType)
        assert isinstance(field_dict["Order ID"], StringType)
        assert isinstance(field_dict["Sales"], DoubleType)
        assert isinstance(field_dict["Quantity"], IntegerType)

    def test_read_csv_files_success(
        self, etl_job_with_mock_snowflake, temp_csv_directory
    ):
        """Test successful reading of CSV files."""
        input_path = f"file://{temp_csv_directory}/*.csv"

        df = etl_job_with_mock_snowflake.read_csv_files(input_path)

        assert isinstance(df, DataFrame)
        assert df.count() > 0
        assert "source_file" in df.columns

        # Verify that source_file column contains file paths
        source_files = [
            row["source_file"] for row in df.select("source_file").distinct().collect()
        ]
        assert len(source_files) > 0
        assert all("sales_data_" in file for file in source_files)

    def test_read_csv_files_empty_directory(self, etl_job_with_mock_snowflake):
        """Test reading from directory with no CSV files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = f"file://{temp_dir}/*.csv"

            df = etl_job_with_mock_snowflake.read_csv_files(input_path)

            assert isinstance(df, DataFrame)
            assert df.count() == 0

    def test_read_csv_files_invalid_path(self, etl_job_with_mock_snowflake):
        """Test reading from invalid path raises exception."""
        input_path = "file:///nonexistent/path/*.csv"

        with pytest.raises(Exception):
            etl_job_with_mock_snowflake.read_csv_files(input_path)

    def test_clean_and_transform_valid_data(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test data cleaning and transformation with valid data."""
        # Create DataFrame from sample data
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        batch_id = "test_batch_001"

        result_df = etl_job_with_mock_snowflake.clean_and_transform(input_df, batch_id)

        # Check that result is a DataFrame
        assert isinstance(result_df, DataFrame)

        # Check that new columns are added
        expected_columns = [
            "unit_price",
            "profit_margin",
            "ingestion_timestamp",
            "source_system",
            "batch_id",
            "partition_date",
        ]
        for col_name in expected_columns:
            assert col_name in result_df.columns

        # Check that batch_id is set correctly
        batch_ids = [
            row["batch_id"] for row in result_df.select("batch_id").distinct().collect()
        ]
        assert len(batch_ids) == 1
        assert batch_ids[0] == batch_id

        # Check that source_system is set to "BATCH"
        source_systems = [
            row["source_system"]
            for row in result_df.select("source_system").distinct().collect()
        ]
        assert len(source_systems) == 1
        assert source_systems[0] == "BATCH"

    def test_clean_and_transform_edge_cases(
        self, etl_job_with_mock_snowflake, spark_session, edge_case_data
    ):
        """Test data cleaning with edge cases (nulls, negatives, etc.)."""
        input_df = create_spark_dataframe(spark_session, edge_case_data)
        batch_id = "test_batch_edge"

        result_df = etl_job_with_mock_snowflake.clean_and_transform(input_df, batch_id)

        # Check that null Order ID records are filtered out
        order_ids = [row["Order ID"] for row in result_df.select("Order ID").collect()]
        assert None not in order_ids

        # Check that negative sales are handled (converted to 0 or filtered)
        sales_values = [row["Sales"] for row in result_df.select("Sales").collect()]
        assert all(sales >= 0 for sales in sales_values if sales is not None)

        # Check that zero quantities are handled (converted to 1)
        quantity_values = [
            row["Quantity"] for row in result_df.select("Quantity").collect()
        ]
        assert all(qty > 0 for qty in quantity_values if qty is not None)

        # Verify derived fields are calculated correctly
        unit_prices = [
            row["unit_price"] for row in result_df.select("unit_price").collect()
        ]
        assert all(price >= 0 for price in unit_prices if price is not None)

    def test_clean_and_transform_text_normalization(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test that text fields are properly normalized (trimmed, uppercased)."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "  ORDER-001  ",  # With spaces
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "  CUST-001  ",  # With spaces
                "Customer Name": "Test Customer",
                "Segment": "Consumer",
                "Country": "  united states  ",  # Lowercase with spaces
                "City": "  Test City  ",
                "State": "  test state  ",  # Lowercase
                "Postal Code": "12345",
                "Region": "  west  ",  # Lowercase
                "Product ID": "  PROD-001  ",  # With spaces
                "Category": "  technology  ",  # Lowercase
                "Sub-Category": "Phones",
                "Product Name": "Test Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 20.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        result_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )

        row = result_df.collect()[0]

        # Check trimming
        assert row["Order ID"] == "ORDER-001"
        assert row["Customer ID"] == "CUST-001"
        assert row["Product ID"] == "PROD-001"

        # Check uppercasing
        assert row["Category"] == "TECHNOLOGY"
        assert row["Region"] == "WEST"
        assert row["Country"] == "UNITED STATES"
        assert row["State"] == "TEST STATE"

    def test_map_to_snowflake_schema(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test mapping DataFrame columns to Snowflake schema."""
        # First clean and transform the data
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )

        # Then map to Snowflake schema
        result_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)

        # Check that all expected Snowflake columns are present
        expected_columns = [
            "ORDER_ID",
            "STORE_ID",
            "PRODUCT_ID",
            "PRODUCT_NAME",
            "CATEGORY",
            "QUANTITY",
            "UNIT_PRICE",
            "TOTAL_PRICE",
            "ORDER_DATE",
            "SHIP_DATE",
            "SALES",
            "PROFIT",
            "CUSTOMER_SEGMENT",
            "REGION",
            "COUNTRY",
            "STATE",
            "CITY",
            "SOURCE_FILE",
            "BATCH_ID",
            "INGESTION_TIMESTAMP",
            "SOURCE_SYSTEM",
            "PARTITION_DATE",
        ]

        for col_name in expected_columns:
            assert col_name in result_df.columns, f"Missing column: {col_name}"

        # Check that Customer ID is mapped to STORE_ID
        store_ids = [row["STORE_ID"] for row in result_df.select("STORE_ID").take(5)]
        customer_ids = [
            row["Customer ID"] for row in cleaned_df.select("Customer ID").take(5)
        ]
        assert store_ids == customer_ids

    def test_write_to_snowflake_mock(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test writing to Snowflake with mocked write operation."""
        # Prepare data
        input_df = create_spark_dataframe(
            spark_session, sample_sales_data[:10]
        )  # Small dataset
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )
        final_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)

        # Mock the DataFrame write operation
        with patch.object(final_df.write, "format") as mock_format:
            mock_format.return_value.options.return_value.option.return_value.mode.return_value.save = (
                Mock()
            )

            # This should not raise an exception
            etl_job_with_mock_snowflake.write_to_snowflake(final_df, "TEST_TABLE")

            # Verify that the write chain was called
            mock_format.assert_called_once_with("snowflake")

    def test_write_to_snowflake_error_handling(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test error handling in write_to_snowflake method."""
        # Prepare data
        input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )
        final_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)

        # Mock the write operation to raise an exception
        with patch.object(final_df.write, "format") as mock_format:
            mock_format.side_effect = Exception("Snowflake connection failed")

            with pytest.raises(Exception, match="Snowflake connection failed"):
                etl_job_with_mock_snowflake.write_to_snowflake(final_df, "TEST_TABLE")

    def test_run_etl_success(self, etl_job_with_mock_snowflake, temp_csv_directory):
        """Test complete ETL process execution."""
        input_path = f"file://{temp_csv_directory}/*.csv"
        output_table = "TEST_SALES_TABLE"
        batch_id = "test_batch_complete"

        # Mock the Snowflake write operation
        with patch.object(
            etl_job_with_mock_snowflake, "write_to_snowflake"
        ) as mock_write:
            result = etl_job_with_mock_snowflake.run_etl(
                input_path, output_table, batch_id
            )

            # Check result structure
            assert result["status"] == "success"
            assert result["batch_id"] == batch_id
            assert "records_processed" in result
            assert "duration_seconds" in result
            assert "start_time" in result
            assert "end_time" in result

            # Check that records were processed
            assert result["records_processed"] > 0

            # Verify Snowflake write was called
            mock_write.assert_called_once()

    def test_run_etl_auto_batch_id(
        self, etl_job_with_mock_snowflake, temp_csv_directory
    ):
        """Test ETL process with auto-generated batch ID."""
        input_path = f"file://{temp_csv_directory}/*.csv"
        output_table = "TEST_SALES_TABLE"

        with patch.object(etl_job_with_mock_snowflake, "write_to_snowflake"):
            result = etl_job_with_mock_snowflake.run_etl(input_path, output_table)

            # Check that batch_id was auto-generated
            assert result["batch_id"].startswith("batch_")
            assert len(result["batch_id"]) > 10  # Should contain timestamp

    def test_run_etl_failure_handling(
        self, etl_job_with_mock_snowflake, temp_csv_directory
    ):
        """Test ETL process failure handling and error reporting."""
        input_path = f"file://{temp_csv_directory}/*.csv"
        output_table = "TEST_SALES_TABLE"
        batch_id = "test_batch_failure"

        # Mock write_to_snowflake to raise an exception
        with patch.object(
            etl_job_with_mock_snowflake, "write_to_snowflake"
        ) as mock_write:
            mock_write.side_effect = Exception("Snowflake write failed")

            with pytest.raises(Exception, match="Snowflake write failed"):
                etl_job_with_mock_snowflake.run_etl(input_path, output_table, batch_id)

    def test_discount_percentage_conversion(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test that discount percentages > 1.0 are converted to decimal form."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "ORDER-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Test Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Test City",
                "State": "Test State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Test Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 15.0,  # 15% as percentage (should be converted to 0.15)
                "Profit": 20.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        result_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )

        discount_value = result_df.select("Discount").collect()[0]["Discount"]
        assert abs(discount_value - 0.15) < 0.001  # Should be converted to decimal

    def test_profit_margin_calculation(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test that profit margin is calculated correctly."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "ORDER-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Test Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Test City",
                "State": "Test State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Test Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 25.0,  # 25% margin
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        result_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )

        profit_margin = result_df.select("profit_margin").collect()[0]["profit_margin"]
        expected_margin = 25.0 / 100.0  # 0.25
        assert abs(profit_margin - expected_margin) < 0.001

    def test_unit_price_calculation(self, etl_job_with_mock_snowflake, spark_session):
        """Test that unit price is calculated correctly."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "ORDER-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Test Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Test City",
                "State": "Test State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Test Product",
                "Sales": 150.0,
                "Quantity": 3,
                "Discount": 0.1,
                "Profit": 25.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        result_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "test_batch"
        )

        unit_price = result_df.select("unit_price").collect()[0]["unit_price"]
        expected_unit_price = 150.0 / 3.0  # 50.0
        assert abs(unit_price - expected_unit_price) < 0.001


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestCreateSparkSession:
    """Test suite for create_spark_session function."""

    def test_create_spark_session_default(self):
        """Test creating Spark session with default parameters."""
        spark = create_spark_session()

        try:
            assert spark is not None
            assert spark.sparkContext.appName == "SalesETL"

            # Check that adaptive query execution is enabled
            assert spark.conf.get("spark.sql.adaptive.enabled") == "true"

        finally:
            spark.stop()

    def test_create_spark_session_custom_name(self):
        """Test creating Spark session with custom app name."""
        app_name = "CustomETLTest"
        spark = create_spark_session(app_name)

        try:
            assert spark.sparkContext.appName == app_name
        finally:
            spark.stop()

    def test_spark_packages_configured(self):
        """Test that required packages are configured in Spark session."""
        spark = create_spark_session()

        try:
            # Check that Snowflake packages are configured
            packages = spark.conf.get("spark.jars.packages")
            assert "snowflake-jdbc" in packages
            assert "spark-snowflake" in packages

        finally:
            spark.stop()


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestETLJobIntegration:
    """Integration tests for ETL job components working together."""

    def test_full_pipeline_data_consistency(
        self, etl_job_with_mock_snowflake, temp_csv_directory
    ):
        """Test that data remains consistent through the full pipeline."""
        input_path = f"file://{temp_csv_directory}/*.csv"

        # Run each step separately to verify data consistency
        raw_df = etl_job_with_mock_snowflake.read_csv_files(input_path)
        initial_count = raw_df.count()

        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            raw_df, "consistency_test"
        )

        final_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)
        final_count = final_df.count()

        # Data should be filtered but final count should be reasonable
        assert final_count > 0
        assert final_count <= initial_count  # Some records may be filtered

        # Check that all required fields are present in final output
        required_snowflake_fields = [
            "ORDER_ID",
            "STORE_ID",
            "PRODUCT_ID",
            "SALES",
            "QUANTITY",
        ]
        for field in required_snowflake_fields:
            assert field in final_df.columns

    def test_multiple_csv_files_aggregation(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test that multiple CSV files are properly aggregated."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple CSV files with different data
            for i in range(3):
                df = pd.DataFrame(sample_sales_data[i * 20 : (i + 1) * 20])
                filepath = os.path.join(temp_dir, f"region_{i+1}_sales.csv")
                df.to_csv(filepath, index=False)

            input_path = f"file://{temp_dir}/*.csv"
            raw_df = etl_job_with_mock_snowflake.read_csv_files(input_path)

            # Check that data from all files is included
            source_files = [
                row["source_file"]
                for row in raw_df.select("source_file").distinct().collect()
            ]
            assert len(source_files) == 3

            # Check total record count
            total_count = raw_df.count()
            assert total_count == 60  # 20 records * 3 files

    def test_empty_files_handling(self, etl_job_with_mock_snowflake, spark_session):
        """Test handling of empty CSV files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create an empty CSV file with headers only
            empty_df = pd.DataFrame(
                columns=[
                    "Row ID",
                    "Order ID",
                    "Order Date",
                    "Ship Date",
                    "Ship Mode",
                    "Customer ID",
                    "Customer Name",
                    "Segment",
                    "Country",
                    "City",
                    "State",
                    "Postal Code",
                    "Region",
                    "Product ID",
                    "Category",
                    "Sub-Category",
                    "Product Name",
                    "Sales",
                    "Quantity",
                    "Discount",
                    "Profit",
                ]
            )
            filepath = os.path.join(temp_dir, "empty_sales.csv")
            empty_df.to_csv(filepath, index=False)

            input_path = f"file://{temp_dir}/*.csv"
            raw_df = etl_job_with_mock_snowflake.read_csv_files(input_path)

            # Should handle empty files gracefully
            assert raw_df.count() == 0

            # Transformation should also handle empty DataFrames
            cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
                raw_df, "empty_test"
            )
            assert cleaned_df.count() == 0

            final_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)
            assert final_df.count() == 0
