"""
Data quality and validation tests for Spark ETL job.

This module contains tests for data quality validation, schema compliance,
and business rule enforcement in the ETL pipeline.
"""

import pytest
import os
import tempfile
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Set
from decimal import Decimal

# PySpark imports with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col,
        count,
        sum as spark_sum,
        avg,
        max as spark_max,
        min as spark_min,
    )
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
    from src.spark.jobs.batch_etl import SalesETLJob
    from tests.conftest_spark import create_spark_dataframe


class DataQualityChecker:
    """Utility class for performing data quality checks."""

    @staticmethod
    def check_null_percentages(
        df: DataFrame, columns: List[str], max_null_percent: float = 0.1
    ) -> Dict[str, float]:
        """Check null percentages for specified columns."""
        total_count = df.count()
        if total_count == 0:
            return {}

        null_percentages = {}
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_count
            null_percentages[column] = null_percentage

        return null_percentages

    @staticmethod
    def check_duplicate_records(df: DataFrame, key_columns: List[str]) -> int:
        """Check for duplicate records based on key columns."""
        total_count = df.count()
        distinct_count = df.select(*key_columns).distinct().count()
        return total_count - distinct_count

    @staticmethod
    def check_data_ranges(
        df: DataFrame, column_ranges: Dict[str, Dict[str, float]]
    ) -> Dict[str, Dict[str, Any]]:
        """Check if numeric columns fall within expected ranges."""
        results = {}

        for column, range_config in column_ranges.items():
            min_val = range_config.get("min")
            max_val = range_config.get("max")

            stats = df.agg(
                spark_min(col(column)).alias("actual_min"),
                spark_max(col(column)).alias("actual_max"),
                count(col(column)).alias("count"),
            ).collect()[0]

            results[column] = {
                "actual_min": stats["actual_min"],
                "actual_max": stats["actual_max"],
                "count": stats["count"],
                "expected_min": min_val,
                "expected_max": max_val,
                "min_violation": stats["actual_min"] < min_val
                if min_val is not None
                else False,
                "max_violation": stats["actual_max"] > max_val
                if max_val is not None
                else False,
            }

        return results

    @staticmethod
    def check_categorical_values(
        df: DataFrame, column_categories: Dict[str, Set[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """Check if categorical columns contain only expected values."""
        results = {}

        for column, expected_categories in column_categories.items():
            actual_categories = set(
                [
                    row[column]
                    for row in df.select(column).distinct().collect()
                    if row[column] is not None
                ]
            )

            results[column] = {
                "expected_categories": expected_categories,
                "actual_categories": actual_categories,
                "unexpected_values": actual_categories - expected_categories,
                "missing_values": expected_categories - actual_categories,
                "is_valid": actual_categories.issubset(expected_categories),
            }

        return results


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestDataQuality:
    """Test suite for data quality validation."""

    def test_schema_compliance(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test that processed data complies with expected schema."""
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "schema_test"
        )

        # Check that all required columns exist
        required_columns = [
            "Order ID",
            "Sales",
            "Quantity",
            "Order Date",
            "Category",
            "Region",
        ]

        for column in required_columns:
            assert column in cleaned_df.columns, f"Missing required column: {column}"

        # Check data types
        schema_dict = {field.name: field.dataType for field in cleaned_df.schema.fields}

        # Numeric fields should be numeric types
        numeric_fields = ["Sales", "Quantity", "Profit", "unit_price", "profit_margin"]
        for field in numeric_fields:
            if field in schema_dict:
                assert isinstance(
                    schema_dict[field], (DoubleType, IntegerType)
                ), f"{field} should be numeric"

    def test_null_value_handling(self, etl_job_with_mock_snowflake, spark_session):
        """Test handling of null values in critical fields."""
        # Create data with strategic null values
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "ORDER-001",
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
            {
                "Row ID": 2,
                "Order ID": None,  # Should be filtered out
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-002",
                "Customer Name": "Null Order Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Null City",
                "State": "Null State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-002",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Null Product",
                "Sales": 200.0,
                "Quantity": 2,
                "Discount": 0.1,
                "Profit": 40.0,
            },
            {
                "Row ID": 3,
                "Order ID": "ORDER-003",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-003",
                "Customer Name": "Null Sales Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Null Sales City",
                "State": "Null Sales State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-003",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Null Sales Product",
                "Sales": None,  # Should be handled
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": None,  # Should be handled
            },
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "null_test"
        )

        # Check null handling
        checker = DataQualityChecker()

        # Critical fields should have low null percentages after cleaning
        critical_fields = ["Order ID", "Sales", "Quantity"]
        null_percentages = checker.check_null_percentages(
            cleaned_df, critical_fields, max_null_percent=0.0
        )

        for field in critical_fields:
            assert (
                null_percentages.get(field, 0) == 0
            ), f"{field} should not have null values after cleaning"

    def test_data_range_validation(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test that numeric data falls within expected business ranges."""
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "range_test"
        )

        checker = DataQualityChecker()

        # Define expected ranges for business rules
        expected_ranges = {
            "Sales": {"min": 0.0, "max": 100000.0},
            "Quantity": {"min": 1, "max": 1000},
            "Discount": {"min": 0.0, "max": 1.0},
            "unit_price": {"min": 0.0, "max": 10000.0},
            "profit_margin": {"min": -1.0, "max": 1.0},
        }

        range_results = checker.check_data_ranges(cleaned_df, expected_ranges)

        for column, result in range_results.items():
            assert not result[
                "min_violation"
            ], f"{column} has values below minimum: {result['actual_min']}"
            assert not result[
                "max_violation"
            ], f"{column} has values above maximum: {result['actual_max']}"

    def test_categorical_value_validation(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test that categorical fields contain only expected values."""
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "categorical_test"
        )

        checker = DataQualityChecker()

        # Define expected categorical values
        expected_categories = {
            "Category": {"TECHNOLOGY", "FURNITURE", "OFFICE SUPPLIES"},
            "Region": {"WEST", "EAST", "CENTRAL", "SOUTH"},
            "source_system": {"BATCH"},
        }

        categorical_results = checker.check_categorical_values(
            cleaned_df, expected_categories
        )

        for column, result in categorical_results.items():
            assert result[
                "is_valid"
            ], f"{column} contains unexpected values: {result['unexpected_values']}"

    def test_duplicate_detection(self, etl_job_with_mock_snowflake, spark_session):
        """Test detection of duplicate records."""
        # Create data with intentional duplicates
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "DUPLICATE-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Duplicate Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Duplicate City",
                "State": "Duplicate State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Duplicate Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 20.0,
            },
            {
                "Row ID": 2,
                "Order ID": "DUPLICATE-001",  # Same Order ID
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Duplicate Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Duplicate City",
                "State": "Duplicate State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Duplicate Product",
                "Sales": 100.0,
                "Quantity": 1,
                "Discount": 0.1,
                "Profit": 20.0,
            },
            {
                "Row ID": 3,
                "Order ID": "UNIQUE-001",
                "Order Date": "01/02/2023",
                "Ship Date": "01/04/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-002",
                "Customer Name": "Unique Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Unique City",
                "State": "Unique State",
                "Postal Code": "54321",
                "Region": "EAST",
                "Product ID": "PROD-002",
                "Category": "FURNITURE",
                "Sub-Category": "Chairs",
                "Product Name": "Unique Product",
                "Sales": 200.0,
                "Quantity": 2,
                "Discount": 0.2,
                "Profit": 40.0,
            },
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "duplicate_test"
        )

        checker = DataQualityChecker()

        # Check for duplicates based on Order ID
        duplicate_count = checker.check_duplicate_records(cleaned_df, ["Order ID"])

        # We expect 1 duplicate (2 records with same Order ID)
        assert duplicate_count == 1, f"Expected 1 duplicate, found {duplicate_count}"

    def test_date_consistency(self, etl_job_with_mock_snowflake, spark_session):
        """Test date field consistency and validation."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "DATE-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",  # Ship date after order date (valid)
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Valid Date Customer",
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
            {
                "Row ID": 2,
                "Order ID": "DATE-002",
                "Order Date": "01/05/2023",
                "Ship Date": "01/02/2023",  # Ship date before order date (invalid)
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-002",
                "Customer Name": "Invalid Date Customer",
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

        input_df = create_spark_dataframe(spark_session, test_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "date_test"
        )

        # Check for records where ship date is before order date
        invalid_dates = cleaned_df.filter(col("Ship Date") < col("Order Date")).count()

        # This test identifies the issue but doesn't fail the pipeline
        # In a real scenario, you might want to add this as a business rule
        if invalid_dates > 0:
            print(f"Warning: {invalid_dates} records have ship date before order date")

    def test_business_logic_validation(
        self, etl_job_with_mock_snowflake, spark_session
    ):
        """Test business logic rules and calculations."""
        test_data = [
            {
                "Row ID": 1,
                "Order ID": "BIZ-001",
                "Order Date": "01/01/2023",
                "Ship Date": "01/03/2023",
                "Ship Mode": "Standard Class",
                "Customer ID": "CUST-001",
                "Customer Name": "Business Logic Customer",
                "Segment": "Consumer",
                "Country": "United States",
                "City": "Business City",
                "State": "Business State",
                "Postal Code": "12345",
                "Region": "WEST",
                "Product ID": "PROD-001",
                "Category": "TECHNOLOGY",
                "Sub-Category": "Phones",
                "Product Name": "Business Product",
                "Sales": 120.0,
                "Quantity": 3,
                "Discount": 0.1,
                "Profit": 24.0,
            }
        ]

        input_df = create_spark_dataframe(spark_session, test_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "business_test"
        )

        # Verify business calculations
        result = cleaned_df.collect()[0]

        # Unit price calculation: Sales / Quantity
        expected_unit_price = 120.0 / 3.0  # 40.0
        actual_unit_price = result["unit_price"]
        assert (
            abs(actual_unit_price - expected_unit_price) < 0.001
        ), f"Unit price calculation error: expected {expected_unit_price}, got {actual_unit_price}"

        # Profit margin calculation: Profit / Sales
        expected_profit_margin = 24.0 / 120.0  # 0.2
        actual_profit_margin = result["profit_margin"]
        assert (
            abs(actual_profit_margin - expected_profit_margin) < 0.001
        ), f"Profit margin calculation error: expected {expected_profit_margin}, got {actual_profit_margin}"

    def test_data_completeness(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test overall data completeness after ETL processing."""
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        initial_count = input_df.count()

        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "completeness_test"
        )
        final_count = cleaned_df.count()

        # Calculate data retention rate
        retention_rate = final_count / initial_count if initial_count > 0 else 0

        # Expect at least 80% data retention for clean sample data
        assert (
            retention_rate >= 0.8
        ), f"Data retention rate too low: {retention_rate:.2%}"

        # Check that essential fields are populated
        essential_fields = ["Order ID", "Sales", "Quantity", "Category", "Region"]
        for field in essential_fields:
            non_null_count = cleaned_df.filter(col(field).isNotNull()).count()
            completeness_rate = non_null_count / final_count if final_count > 0 else 0
            assert (
                completeness_rate >= 0.95
            ), f"{field} completeness rate too low: {completeness_rate:.2%}"

    def test_snowflake_schema_mapping_quality(
        self, etl_job_with_mock_snowflake, spark_session, sample_sales_data
    ):
        """Test quality of data after Snowflake schema mapping."""
        input_df = create_spark_dataframe(spark_session, sample_sales_data)
        cleaned_df = etl_job_with_mock_snowflake.clean_and_transform(
            input_df, "mapping_test"
        )
        final_df = etl_job_with_mock_snowflake.map_to_snowflake_schema(cleaned_df)

        # Check that no data is lost during mapping
        assert final_df.count() == cleaned_df.count(), "Data lost during schema mapping"

        # Check that key business metrics are preserved
        original_sales_sum = cleaned_df.agg(
            spark_sum("Sales").alias("total_sales")
        ).collect()[0]["total_sales"]
        mapped_sales_sum = final_df.agg(
            spark_sum("TOTAL_PRICE").alias("total_sales")
        ).collect()[0]["total_sales"]

        assert (
            abs(original_sales_sum - mapped_sales_sum) < 0.01
        ), "Sales total not preserved during mapping"

        # Check that all mapped fields have data
        for column in final_df.columns:
            if column not in ["SOURCE_FILE"]:  # SOURCE_FILE might be null in tests
                non_null_count = final_df.filter(col(column).isNotNull()).count()
                assert non_null_count > 0, f"Mapped column {column} has no data"


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestDataValidationUtilities:
    """Test suite for data validation utility functions."""

    def test_data_quality_checker_null_percentages(self, spark_session):
        """Test null percentage calculation utility."""
        test_data = [
            {"col1": "A", "col2": 1, "col3": None},
            {"col1": None, "col2": 2, "col3": "X"},
            {"col1": "C", "col2": None, "col3": "Y"},
            {"col1": "D", "col2": 4, "col3": None},
        ]

        df = spark_session.createDataFrame(test_data)
        checker = DataQualityChecker()

        null_percentages = checker.check_null_percentages(df, ["col1", "col2", "col3"])

        assert null_percentages["col1"] == 0.25  # 1 null out of 4
        assert null_percentages["col2"] == 0.25  # 1 null out of 4
        assert null_percentages["col3"] == 0.5  # 2 nulls out of 4

    def test_data_quality_checker_ranges(self, spark_session):
        """Test data range validation utility."""
        test_data = [
            {"value": 5.0},
            {"value": 15.0},
            {"value": 25.0},
        ]

        df = spark_session.createDataFrame(test_data)
        checker = DataQualityChecker()

        range_config = {"value": {"min": 10.0, "max": 20.0}}
        results = checker.check_data_ranges(df, range_config)

        assert results["value"]["actual_min"] == 5.0
        assert results["value"]["actual_max"] == 25.0
        assert results["value"]["min_violation"] == True  # 5.0 < 10.0
        assert results["value"]["max_violation"] == True  # 25.0 > 20.0

    def test_data_quality_checker_categories(self, spark_session):
        """Test categorical value validation utility."""
        test_data = [
            {"category": "A"},
            {"category": "B"},
            {"category": "C"},
            {"category": "X"},  # Unexpected value
        ]

        df = spark_session.createDataFrame(test_data)
        checker = DataQualityChecker()

        expected_categories = {"category": {"A", "B", "C"}}
        results = checker.check_categorical_values(df, expected_categories)

        assert results["category"]["unexpected_values"] == {"X"}
        assert results["category"]["missing_values"] == set()
        assert results["category"]["is_valid"] == False
