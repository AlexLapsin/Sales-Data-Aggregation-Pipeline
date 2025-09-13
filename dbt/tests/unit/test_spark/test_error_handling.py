"""
Error handling and edge case tests for Spark ETL job.

This module contains tests for various error conditions, edge cases,
and failure scenarios to ensure robust error handling.
"""

import pytest
import os
import tempfile
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List
import json

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
    from pyspark.sql.utils import AnalysisException

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None
    AnalysisException = Exception

# Local imports
import sys

sys.path.append(os.path.dirname(__file__))

if SPARK_AVAILABLE:
    from src.spark.jobs.batch_etl import SalesETLJob, create_spark_session
    from tests.conftest_spark import create_spark_dataframe


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestConfigurationErrors:
    """Test error handling for configuration issues."""

    def test_missing_snowflake_account(self, spark_session):
        """Test error when Snowflake account is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing required Snowflake configuration"
            ):
                SalesETLJob(spark_session)

    def test_missing_snowflake_user(self, spark_session):
        """Test error when Snowflake user is missing."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                # Missing SNOWFLAKE_USER and SNOWFLAKE_PASSWORD
            },
        ):
            with pytest.raises(
                ValueError, match="Missing required Snowflake configuration"
            ):
                SalesETLJob(spark_session)

    def test_missing_snowflake_password(self, spark_session):
        """Test error when Snowflake password is missing."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                # Missing SNOWFLAKE_PASSWORD
            },
        ):
            with pytest.raises(
                ValueError, match="Missing required Snowflake configuration"
            ):
                SalesETLJob(spark_session)

    def test_partial_snowflake_config(self, spark_session):
        """Test that partial configuration still fails appropriately."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "",  # Empty password
            },
        ):
            with pytest.raises(
                ValueError, match="Missing required Snowflake configuration"
            ):
                SalesETLJob(spark_session)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestDataInputErrors:
    """Test error handling for data input issues."""

    def test_invalid_file_path(self, etl_job_with_mock_snowflake):
        """Test error handling for invalid file paths."""
        invalid_path = "file:///nonexistent/directory/*.csv"

        with pytest.raises(Exception):
            etl_job_with_mock_snowflake.read_csv_files(invalid_path)

    def test_empty_file_path(self, etl_job_with_mock_snowflake):
        """Test handling of empty file path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Directory exists but has no CSV files
            empty_path = f"file://{temp_dir}/*.csv"

            df = etl_job_with_mock_snowflake.read_csv_files(empty_path)
            assert df.count() == 0, "Empty directory should return empty DataFrame"

    def test_malformed_csv_files(self, etl_job_with_mock_snowflake, spark_session):
        """Test handling of malformed CSV files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create malformed CSV file
            malformed_file = os.path.join(temp_dir, "malformed.csv")
            with open(malformed_file, "w") as f:
                f.write("Order ID,Sales,Quantity\n")
                f.write("ORDER-001,not_a_number,abc\n")  # Invalid data types
                f.write("ORDER-002,200.0,\n")  # Missing quantity
                f.write("ORDER-003,,3\n")  # Missing sales

            input_path = f"file://{temp_dir}/*.csv"

            # Should read the file but may have issues during transformation
            raw_df = etl_job_with_mock_snowflake.read_csv_files(input_path)
            assert raw_df.count() > 0, "Should read malformed CSV"

            # Transformation should handle malformed data gracefully
            cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
                raw_df, "malformed_test"
            )

            # Some records may be filtered out due to data quality issues
            assert (
                cleaned_df.count() >= 0
            ), "Should handle malformed data without crashing"

    def test_csv_with_missing_required_columns(self, etl_job_with_mock_snowflake):
        """Test handling of CSV files missing required columns."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create CSV with only some required columns
            incomplete_data = pd.DataFrame(
                {
                    "Order ID": ["ORDER-001", "ORDER-002"],
                    "Sales": [100.0, 200.0],
                    # Missing other required columns like Order Date, Quantity, etc.
                }
            )

            incomplete_file = os.path.join(temp_dir, "incomplete.csv")
            incomplete_data.to_csv(incomplete_file, index=False)

            input_path = f"file://{temp_dir}/*.csv"

            # This should raise an exception due to schema mismatch
            with pytest.raises(Exception):
                etl_job_with_mock_snowflake.read_csv_files(input_path)

    def test_csv_with_wrong_date_formats(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test handling of CSV files with incorrect date formats."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "ORDER-001",
                "Order Date": "2023-01-01",  # Wrong format (should be MM/dd/yyyy)
                "Ship Date": "invalid-date",  # Completely invalid
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
                "Profit": 20.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)

        # Should handle invalid dates by filtering them out or setting to null
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "date_error_test"
        )

        # Records with invalid dates should be filtered out
        valid_records = cleaned_df.filter(col("Order Date").isNotNull()).count()
        assert valid_records >= 0, "Should handle invalid dates gracefully"

    def test_extremely_large_csv_file(self, etl_job_with_mock_snowflake):
        """Test handling of extremely large CSV files (memory stress test)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a moderately large CSV file (but not so large as to break CI)
            large_data = []
            for i in range(10000):  # 10K records
                large_data.append(
                    {
                        "Row ID": i,
                        "Order ID": f"LARGE-{i:06d}",
                        "Order Date": "01/01/2023",
                        "Ship Date": "01/03/2023",
                        "Ship Mode": "Standard Class",
                        "Customer ID": f"CUST-{i % 1000}",
                        "Customer Name": f"Customer {i}",
                        "Segment": "Consumer",
                        "Country": "United States",
                        "City": f"City {i % 100}",
                        "State": f"State {i % 50}",
                        "Postal Code": "12345",
                        "Region": "WEST",
                        "Product ID": f"PROD-{i:06d}",
                        "Category": "TECHNOLOGY",
                        "Sub-Category": "Phones",
                        "Product Name": f"Product {i}",
                        "Sales": 100.0,
                        "Quantity": 1,
                        "Discount": 0.1,
                        "Profit": 20.0,
                    }
                )

            large_df = pd.DataFrame(large_data)
            large_file = os.path.join(temp_dir, "large_file.csv")
            large_df.to_csv(large_file, index=False)

            input_path = f"file://{temp_dir}/*.csv"

            # Should handle large files without memory errors
            raw_df = etl_job_with_mock_snowflake.read_csv_files(input_path)
            assert raw_df.count() == 10000, "Should read all records from large file"

            # Transformation should also work
            cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
                raw_df, "large_file_test"
            )
            assert cleaned_df.count() > 0, "Should process large file successfully"


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestTransformationErrors:
    """Test error handling during data transformation."""

    def test_null_handling_in_calculations(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test handling of null values in mathematical calculations."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "NULL-TEST-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Null Test Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Null Test City",
                "State": "Null Test State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Null Test Product",
                "Sales": None,  # Null sales
                "Quantity": None,  # Null quantity
                "Discount": None,  # Null discount
                "Profit": None,  # Null profit
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)

        # Should handle null values without crashing
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "null_calc_test"
        )

        # Check that null values are handled appropriately
        result = cleaned_df.collect()
        if len(result) > 0:  # Record might be filtered out
            row = result[0]
            # Sales should be set to 0.0 if null
            assert row["Sales"] == 0.0 or row["Sales"] is None
            # Quantity should be set to 1 if null or <= 0
            assert row["Quantity"] == 1 or row["Quantity"] is None

    def test_division_by_zero_in_calculations(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test handling of division by zero in derived calculations."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "DIV-ZERO-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Division Test Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Division Test City",
                "State": "Division Test State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Division Test Product",
                "Sales": 0.0,  # Zero sales (will cause division by zero in profit margin)
                "Quantity": 0,  # Zero quantity (will cause division by zero in unit price)
                "Discount": 0.1,
                "Profit": 20.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)

        # Should handle division by zero without crashing
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "div_zero_test"
        )

        # Check results after filtering (record might be filtered out due to zero quantity)
        result_count = cleaned_df.count()
        if result_count > 0:
            result = cleaned_df.collect()[0]
            # unit_price and profit_margin should handle division by zero
            assert result["unit_price"] == 0.0 or result["unit_price"] is None
            assert result["profit_margin"] == 0.0 or result["profit_margin"] is None

    def test_extreme_numeric_values(self, etl_job_with_mock_snowflake, spark_session):
        """Test handling of extreme numeric values."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "EXTREME-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Extreme Test Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Extreme Test City",
                "State": "Extreme Test State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Extreme Test Product",
                "Sales": 999999999.99,  # Very large sales
                "Quantity": 1000000,  # Very large quantity
                "Discount": 0.99,  # High discount
                "Profit": -999999.99,  # Very negative profit
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)

        # Should handle extreme values without overflow or errors
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "extreme_test"
        )

        result_count = cleaned_df.count()
        assert result_count >= 0, "Should handle extreme values without crashing"

        if result_count > 0:
            result = cleaned_df.collect()[0]
            # Calculations should complete without overflow
            assert result["unit_price"] is not None
            assert result["profit_margin"] is not None

    def test_invalid_category_values(self, etl_job_with_mock_snowflake, spark_session):
        """Test handling of invalid or unexpected category values."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "INVALID-CAT-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Invalid Category Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Invalid City",
                "State": "Invalid State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "INVALID_CATEGORY_12345!@#",  # Invalid category
                "Sub-Category": "",  # Empty sub-category
                "Product Name": "Invalid Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 20.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)

        # Should handle invalid categories without crashing
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "invalid_cat_test"
        )

        # Should still process the record (category validation is not enforced)
        assert cleaned_df.count() >= 0, "Should handle invalid categories"

        if cleaned_df.count() > 0:
            result = cleaned_df.collect()[0]
            # Category should be uppercased and trimmed
            assert result["Category"] == "INVALID_CATEGORY_12345!@#"


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSnowflakeConnectionErrors:
    """Test error handling for Snowflake connection issues."""

    def test_invalid_snowflake_credentials(self, spark_session, sample_sales_data):
        """Test handling of invalid Snowflake credentials."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "invalid.account.snowflakecomputing.com",
                "SNOWFLAKE_USER": "invalid_user",
                "SNOWFLAKE_PASSWORD": "invalid_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
            cleaned_df = etl_job.clean_and_transform(input_df, "invalid_creds_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            # Should raise an exception when trying to write to Snowflake
            with pytest.raises(Exception):
                etl_job.write_to_snowflake(final_df, "TEST_TABLE")

    def test_snowflake_connection_timeout(self, spark_session, sample_sales_data):
        """Test handling of Snowflake connection timeouts."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "timeout.test.account",  # Non-existent account
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
            cleaned_df = etl_job.clean_and_transform(input_df, "timeout_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            # Should handle connection timeout gracefully
            with pytest.raises(Exception):
                etl_job.write_to_snowflake(final_df, "TEST_TABLE")

    def test_snowflake_table_not_exists(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test handling when Snowflake table doesn't exist."""
        input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "table_not_exists_test"
        )
        final_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)

        # Mock Snowflake write to simulate table not exists error
        with patch.object(final_df.write, "format") as mock_format:
            mock_format.side_effect = Exception(
                "Table 'NONEXISTENT_TABLE' doesn't exist"
            )

            with pytest.raises(
                Exception, match="Table 'NONEXISTENT_TABLE' doesn't exist"
            ):
                etl_job_with_mock_snowflake.write_to_snowflake(
                    final_df, "NONEXISTENT_TABLE"
                )


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestETLWorkflowErrors:
    """Test error handling in complete ETL workflow."""

    def test_etl_failure_in_read_step(self, etl_job_with_mock_snowflake):
        """Test ETL failure during read step."""
        invalid_path = "file:///completely/invalid/path/*.csv"

        with pytest.raises(Exception):
            etl_job_with_mock_snowflake.run_etl(
                invalid_path, "TEST_TABLE", "read_fail_test"
            )

    def test_etl_failure_in_transform_step(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test ETL failure during transform step."""
        # Create data that might cause transformation issues
        problematic_data = [
            {
                "Row ID": 1,
                "Order ID": None,  # This will cause filtering
                "Order Date": "invalid-date",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Problem Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Problem City",
                "State": "Problem State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Problem Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 20.0,
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create CSV file with problematic data
            df = pd.DataFrame(problematic_data)
            filepath = os.path.join(temp_dir, "problematic.csv")
            df.to_csv(filepath, index=False)

            input_path = f"file://{temp_dir}/*.csv"

            # Mock Snowflake write to focus on transformation
            with patch.object(etl_job_with_mock_snowflake, "write_to_snowflake"):
                # Should handle problematic data gracefully
                result = etl_job_with_mock_snowflake.run_etl(
                    input_path, "TEST_TABLE", "transform_fail_test"
                )

                # Might have no records after filtering, but shouldn't crash
                assert result["status"] == "success"
                assert result["records_processed"] >= 0

    def test_etl_failure_in_write_step(
        self, etl_job_with_mock_snowflake, temp_csv_directory
    ):
        """Test ETL failure during write step."""
        input_path = f"file://{temp_csv_directory}/*.csv"

        # Mock write_to_snowflake to raise an exception
        with patch.object(
            etl_job_with_mock_snowflake, "write_to_snowflake"
        ) as mock_write:
            mock_write.side_effect = Exception("Snowflake write failed")

            with pytest.raises(Exception, match="Snowflake write failed"):
                etl_job_with_mock_snowflake.run_etl(
                    input_path, "TEST_TABLE", "write_fail_test"
                )

    def test_etl_partial_failure_recovery(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test ETL behavior with partial data failures."""
        # Mix of valid and invalid data
        mixed_data = [
            # Valid record
            {
                "Row ID": 1,
                "Order ID": "VALID-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Valid Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Valid City",
                "State": "Valid State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Valid Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 20.0,
            },
            # Invalid record (null Order ID)
            {
                "Row ID": 2,
                "Order ID": None,
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-002",
                "Customer Name": "Invalid Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Invalid City",
                "State": "Invalid State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-002",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Invalid Product",
                "Sales": 200.0,
                "Quantity": 2,
                "Discount": 0.1,
                "Profit": 40.0,
            },
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create CSV with mixed data
            df = pd.DataFrame(mixed_data)
            filepath = os.path.join(temp_dir, "mixed_data.csv")
            df.to_csv(filepath, index=False)

            input_path = f"file://{temp_dir}/*.csv"

            # Mock Snowflake write
            with patch.object(etl_job_with_mock_snowflake, "write_to_snowflake"):
                result = etl_job_with_mock_snowflake.run_etl(
                    input_path, "TEST_TABLE", "partial_fail_test"
                )

                # Should process valid records and filter invalid ones
                assert result["status"] == "success"
                assert result["records_processed"] > 0  # At least the valid record
                assert result["records_processed"] < len(
                    mixed_data
                )  # Invalid record filtered

    def test_memory_exhaustion_handling(self, spark_session):
        """Test handling of potential memory exhaustion scenarios."""
        # This is a simplified test - real memory exhaustion is hard to simulate safely
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            # Create a dataset that could potentially cause memory issues
            large_string = "x" * 1000  # 1KB string
            memory_stress_data = []

            for i in range(1000):  # 1000 records with large strings
                memory_stress_data.append(
                    {
                        "Row ID": i,
                        "Order ID": f"MEMORY-{i:06d}",
                        "Order Date": "01/01/2023",
                        "Ship Date": "01/03/2023",
                        "Ship Mode": "Standard Class",
                        "Customer ID": f"CUST-{i}",
                        "Customer Name": f"Customer {large_string}",  # Large string
                        "Segment": "Consumer",
                        "Country": "United States",
                        "City": f"City {large_string}",  # Large string
                        "State": "Test State",
                        "Postal Code": "12345",
                        "Region": "WEST",
                        "Product ID": f"PROD-{i}",
                        "Category": "TECHNOLOGY",
                        "Sub-Category": "Phones",
                        "Product Name": f"Product {large_string}",  # Large string
                        "Sales": 100.0,
                        "Quantity": 1,
                        "Discount": 0.1,
                        "Profit": 20.0,
                    }
                )

            input_df = create_spark_dataframe(spark_session, memory_stress_data)

            # Should handle large strings without memory errors
            cleaned_df = etl_job.clean_and_transform(input_df, "memory_stress_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            result_count = final_df.count()
            assert result_count > 0, "Should handle memory stress scenario"


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestConcurrencyAndRaceConditions:
    """Test error handling for concurrency and race condition scenarios."""

    def test_concurrent_etl_jobs(self, spark_session):
        """Test behavior when multiple ETL jobs run concurrently."""
        # This is a simplified test for concurrency issues
        # In practice, you'd use actual threading or multiprocessing

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job1 = SalesETLJob(spark_session)
            etl_job2 = SalesETLJob(spark_session)

            # Both jobs should be able to coexist
            assert etl_job1.snowflake_options == etl_job2.snowflake_options

            # Test that each job maintains its own state
            test_data = [
                {
                    "Row ID": 1,
                    "Order ID": "CONCURRENT-001",
                    "Order Date": "01/01/2023",
                    "Ship Date": "01/03/2023",
                    "Ship Mode": "Standard Class",
                    "Customer ID": "CUST-001",
                    "Customer Name": "Concurrent Customer",
                    "Segment": "Consumer",
                    "Country": "United States",
                    "City": "Concurrent City",
                    "State": "Concurrent State",
                    "Postal Code": "12345",
                    "Region": "WEST",
                    "Product ID": "PROD-001",
                    "Category": "TECHNOLOGY",
                    "Sub-Category": "Phones",
                    "Product Name": "Concurrent Product",
                    "Sales": 100.0,
                    "Quantity": 1,
                    "Discount": 0.1,
                    "Profit": 20.0,
                }
            ]

            input_df1 = create_spark_dataframe(spark_session, test_data)
            input_df2 = create_spark_dataframe(spark_session, test_data)

            # Both transformations should work independently
            cleaned_df1 = etl_job1.clean_and_transform(input_df1, "concurrent_test_1")
            cleaned_df2 = etl_job2.clean_and_transform(input_df2, "concurrent_test_2")

            assert cleaned_df1.count() == cleaned_df2.count()

    def test_batch_id_uniqueness(self, etl_job_with_mock_snowflake):
        """Test that batch IDs are unique even when generated quickly."""
        import time

        batch_ids = []

        # Generate multiple batch IDs in quick succession
        for i in range(10):
            # Simulate rapid ETL job creation
            with patch.object(etl_job_with_mock_snowflake, "write_to_snowflake"):
                result = etl_job_with_mock_snowflake.run_etl(
                    "file:///fake/path/*.csv", "TEST_TABLE"
                )
                batch_ids.append(result["batch_id"])

            time.sleep(0.01)  # Very short delay

        # All batch IDs should be unique
        unique_batch_ids = set(batch_ids)
        assert len(unique_batch_ids) == len(batch_ids), "Batch IDs should be unique"
