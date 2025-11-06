"""
Pytest configuration and fixtures for Spark ETL tests.

This module provides shared fixtures for testing the Spark ETL job,
including Spark session management, test data generation, and mocking utilities.
"""

import os
import tempfile
import shutil
import pytest
import pandas as pd
from datetime import datetime, timedelta
from typing import Generator, Dict, Any, List
from unittest.mock import Mock, patch
import random
from faker import Faker

# PySpark imports with error handling for environments without Spark
try:
    from pyspark.sql import SparkSession, DataFrame
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
    from spark_config import SparkConfig, get_spark_config
    from sales_batch_job import SalesETLJob


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Create a Spark session for testing with minimal configuration."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    spark = (
        SparkSession.builder.appName("SalesETL-Test")
        .master("local[2]")
        .config(
            "spark.sql.adaptive.enabled", "false"
        )  # Disable for deterministic tests
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())  # Temporary warehouse
        .config("spark.sql.shuffle.partitions", "2")  # Reduce for tests
        .getOrCreate()
    )

    # Set log level to reduce noise in tests
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture
def fake() -> Faker:
    """Faker instance for generating test data."""
    fake = Faker()
    Faker.seed(42)  # For reproducible test data
    return fake


@pytest.fixture
def sample_sales_data(fake: Faker) -> List[Dict[str, Any]]:
    """Generate sample sales data for testing."""
    categories = ["TECHNOLOGY", "FURNITURE", "OFFICE SUPPLIES"]
    sub_categories = {
        "TECHNOLOGY": ["Phones", "Computers", "Machines"],
        "FURNITURE": ["Chairs", "Tables", "Bookcases"],
        "OFFICE SUPPLIES": ["Storage", "Art", "Paper"],
    }
    regions = ["WEST", "EAST", "CENTRAL", "SOUTH"]
    segments = ["Consumer", "Corporate", "Home Office"]
    ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]

    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(100):  # Default 100 records
        category = random.choice(categories)
        sub_category = random.choice(sub_categories[category])
        region = random.choice(regions)

        order_date = start_date + timedelta(days=random.randint(0, 365))
        ship_date = order_date + timedelta(days=random.randint(1, 7))

        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 500), 2)
        sales = round(quantity * unit_price, 2)
        discount = round(random.uniform(0, 0.3), 2)
        profit = round(sales * random.uniform(0.1, 0.4), 2)

        record = {
            "Row ID": i + 1,
            "Order ID": f"ORDER-{i+1:06d}",
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": ship_date.strftime("%m/%d/%Y"),
            "Ship Mode": random.choice(ship_modes),
            "Customer ID": f"CUST-{random.randint(1000, 9999)}",
            "Customer Name": fake.name(),
            "Segment": random.choice(segments),
            "Country": "United States",
            "City": fake.city(),
            "State": fake.state(),
            "Postal Code": fake.zipcode(),
            "Region": region,
            "Product ID": f"{category[:3]}-{i+1:06d}",
            "Category": category,
            "Sub-Category": sub_category,
            "Product Name": f"{sub_category} {fake.word().title()}",
            "Sales": sales,
            "Quantity": quantity,
            "Discount": discount,
            "Profit": profit,
        }
        data.append(record)

    return data


@pytest.fixture
def temp_csv_directory(
    sample_sales_data: List[Dict[str, Any]],
) -> Generator[str, None, None]:
    """Create temporary directory with sample CSV files for testing."""
    temp_dir = tempfile.mkdtemp()

    try:
        # Create multiple CSV files to test multi-file processing
        for i in range(3):
            df = pd.DataFrame(
                sample_sales_data[i * 30 : (i + 1) * 30]
            )  # Split data across files
            if len(df) > 0:  # Only create file if there's data
                filepath = os.path.join(temp_dir, f"sales_data_{i+1}.csv")
                df.to_csv(filepath, index=False)

        yield temp_dir

    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def invalid_csv_directory() -> Generator[str, None, None]:
    """Create temporary directory with invalid CSV files for error testing."""
    temp_dir = tempfile.mkdtemp()

    try:
        # Create CSV with missing required columns
        invalid_data = pd.DataFrame(
            {
                "Order ID": ["ORDER-001", "ORDER-002"],
                "Sales": [100.0, 200.0],
                # Missing other required columns
            }
        )
        filepath = os.path.join(temp_dir, "invalid_data.csv")
        invalid_data.to_csv(filepath, index=False)

        # Create empty CSV
        empty_filepath = os.path.join(temp_dir, "empty_data.csv")
        with open(empty_filepath, "w") as f:
            f.write("Order ID,Sales\n")  # Header only

        # Create malformed CSV
        malformed_filepath = os.path.join(temp_dir, "malformed_data.csv")
        with open(malformed_filepath, "w") as f:
            f.write("Order ID,Sales\n")
            f.write("ORDER-001,not_a_number\n")  # Invalid data type
            f.write("ORDER-002,")  # Missing value

        yield temp_dir

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_snowflake_config():
    """Mock Snowflake configuration for testing."""
    return {
        "sfUrl": "test.snowflake.com",
        "sfUser": "test_user",
        "sfPassword": "test_password",
        "sfDatabase": "TEST_DB",
        "sfSchema": "TEST_SCHEMA",
        "sfWarehouse": "TEST_WH",
        "sfRole": "TEST_ROLE",
    }


@pytest.fixture
def etl_job_with_mock_snowflake(spark_session: SparkSession, mock_snowflake_config):
    """Create SalesETLJob instance with mocked Snowflake configuration."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")

    with patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": mock_snowflake_config["sfUrl"],
            "SNOWFLAKE_USER": mock_snowflake_config["sfUser"],
            "SNOWFLAKE_PASSWORD": mock_snowflake_config["sfPassword"],
            "SNOWFLAKE_DATABASE": mock_snowflake_config["sfDatabase"],
            "SNOWFLAKE_SCHEMA": mock_snowflake_config["sfSchema"],
            "SNOWFLAKE_WAREHOUSE": mock_snowflake_config["sfWarehouse"],
            "SNOWFLAKE_ROLE": mock_snowflake_config["sfRole"],
        },
    ):
        return SalesETLJob(spark_session)


@pytest.fixture
def performance_test_data() -> List[Dict[str, Any]]:
    """Generate larger dataset for performance testing."""
    categories = ["TECHNOLOGY", "FURNITURE", "OFFICE SUPPLIES"]
    regions = ["WEST", "EAST", "CENTRAL", "SOUTH"]

    data = []
    for i in range(10000):  # 10K records for performance tests
        category = random.choice(categories)
        region = random.choice(regions)

        # Generate consistent data for reproducible tests
        random.seed(i)

        order_date = datetime(2023, 1, 1) + timedelta(days=i % 365)
        quantity = random.randint(1, 10)
        sales = round(random.uniform(10, 1000), 2)

        record = {
            "Row ID": i + 1,
            "Order ID": f"PERF-ORDER-{i+1:08d}",
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": (order_date + timedelta(days=2)).strftime("%m/%d/%Y"),
            "Ship Mode": "Standard Class",
            "Customer ID": f"PERF-CUST-{i % 1000:04d}",
            "Customer Name": f"Performance Customer {i % 1000}",
            "Segment": "Consumer",
            "Country": "United States",
            "City": f"City {i % 100}",
            "State": f"State {i % 50}",
            "Postal Code": f"{10000 + (i % 90000)}",
            "Region": region,
            "Product ID": f"PERF-{category[:3]}-{i+1:08d}",
            "Category": category,
            "Sub-Category": "Test SubCategory",
            "Product Name": f"Performance Product {i % 100}",
            "Sales": sales,
            "Quantity": quantity,
            "Discount": 0.1,
            "Profit": round(sales * 0.2, 2),
        }
        data.append(record)

    return data


@pytest.fixture
def edge_case_data() -> List[Dict[str, Any]]:
    """Generate edge case data for testing error handling."""
    return [
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
        # Null Order ID (should be filtered out)
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
        # Negative sales (should be cleaned to 0)
        {
            "Row ID": 3,
            "Order ID": "NEGATIVE-001",
            "Order Date": "01/01/2023",
            "Ship Date": "01/03/2023",
            "Ship Mode": "Standard Class",
            "Customer ID": "CUST-003",
            "Customer Name": "Negative Customer",
            "Segment": "Consumer",
            "Country": "United States",
            "City": "Negative City",
            "State": "Negative State",
            "Postal Code": "12345",
            "Region": "WEST",
            "Product ID": "PROD-003",
            "Category": "TECHNOLOGY",
            "Sub-Category": "Phones",
            "Product Name": "Negative Product",
            "Sales": -100.0,
            "Quantity": 1,
            "Discount": 0.1,
            "Profit": -20.0,
        },
        # Zero quantity (should be cleaned to 1)
        {
            "Row ID": 4,
            "Order ID": "ZERO-QTY-001",
            "Order Date": "01/01/2023",
            "Ship Date": "01/03/2023",
            "Ship Mode": "Standard Class",
            "Customer ID": "CUST-004",
            "Customer Name": "Zero Quantity Customer",
            "Segment": "Consumer",
            "Country": "United States",
            "City": "Zero City",
            "State": "Zero State",
            "Postal Code": "12345",
            "Region": "WEST",
            "Product ID": "PROD-004",
            "Category": "TECHNOLOGY",
            "Sub-Category": "Phones",
            "Product Name": "Zero Quantity Product",
            "Sales": 100.0,
            "Quantity": 0,
            "Discount": 0.1,
            "Profit": 20.0,
        },
        # Invalid date format (should cause parsing issues)
        {
            "Row ID": 5,
            "Order ID": "INVALID-DATE-001",
            "Order Date": "invalid-date",
            "Ship Date": "01/03/2023",
            "Ship Mode": "Standard Class",
            "Customer ID": "CUST-005",
            "Customer Name": "Invalid Date Customer",
            "Segment": "Consumer",
            "Country": "United States",
            "City": "Invalid Date City",
            "State": "Invalid Date State",
            "Postal Code": "12345",
            "Region": "WEST",
            "Product ID": "PROD-005",
            "Category": "TECHNOLOGY",
            "Sub-Category": "Phones",
            "Product Name": "Invalid Date Product",
            "Sales": 100.0,
            "Quantity": 1,
            "Discount": 0.1,
            "Profit": 20.0,
        },
    ]


# Utility functions for tests
def create_spark_dataframe(
    spark: SparkSession, data: List[Dict[str, Any]]
) -> DataFrame:
    """Create Spark DataFrame from test data."""
    if not data:
        # Return empty DataFrame with correct schema
        from sales_batch_job import SalesETLJob

        job = SalesETLJob(spark)
        return spark.createDataFrame([], job.define_schema())

    df = spark.createDataFrame(data)
    return df


def assert_dataframe_equal(df1: DataFrame, df2: DataFrame, check_order: bool = False):
    """Assert that two DataFrames are equal."""
    # Check schemas
    assert df1.schema == df2.schema, "DataFrames have different schemas"

    # Check row counts
    assert df1.count() == df2.count(), "DataFrames have different row counts"

    # Check data content
    if check_order:
        df1_rows = df1.collect()
        df2_rows = df2.collect()
        assert df1_rows == df2_rows, "DataFrames have different data"
    else:
        # Compare without order (useful for partitioned data)
        df1_sorted = df1.orderBy(*df1.columns).collect()
        df2_sorted = df2.orderBy(*df2.columns).collect()
        assert df1_sorted == df2_sorted, "DataFrames have different data"


# Test markers for different test categories
pytest_plugins = []

# Markers for organizing tests
pytestmark = [
    pytest.mark.spark,  # All tests in this module are Spark tests
]
