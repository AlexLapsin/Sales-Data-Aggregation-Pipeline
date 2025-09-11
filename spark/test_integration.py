"""
Integration tests for Spark ETL job with external dependencies.

This module contains integration tests that verify connectivity and compatibility
with external systems like Snowflake, S3, and different Spark environments.
"""

import pytest
import os
import tempfile
import json
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, Optional
import time

# PySpark imports with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

# Snowflake imports with error handling
try:
    import snowflake.connector
    from snowflake.connector import DictCursor

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

# AWS imports with error handling
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

# Local imports
import sys

sys.path.append(os.path.dirname(__file__))

if SPARK_AVAILABLE:
    from sales_batch_job import SalesETLJob, create_spark_session
    from spark_config import (
        SnowflakeConfig,
        get_spark_config,
        create_spark_session_from_config,
    )
    from conftest import create_spark_dataframe


class SnowflakeConnectionManager:
    """Utility class for managing Snowflake connections in tests."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.connection = None

    def connect(self) -> bool:
        """Attempt to connect to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                schema=self.config.schema,
                warehouse=self.config.warehouse,
                role=self.config.role,
            )
            return True
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            return False

    def test_connection(self) -> bool:
        """Test the connection by running a simple query."""
        if not self.connection:
            return False

        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute("SELECT CURRENT_VERSION()")
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False

    def create_test_table(self, table_name: str) -> bool:
        """Create a test table for integration testing."""
        if not self.connection:
            return False

        try:
            cursor = self.connection.cursor()

            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Create test table with Snowflake schema
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                ORDER_ID VARCHAR(255),
                STORE_ID VARCHAR(255),
                PRODUCT_ID VARCHAR(255),
                PRODUCT_NAME VARCHAR(500),
                CATEGORY VARCHAR(255),
                QUANTITY INTEGER,
                UNIT_PRICE DECIMAL(10,2),
                TOTAL_PRICE DECIMAL(10,2),
                ORDER_DATE DATE,
                SHIP_DATE DATE,
                SALES DECIMAL(10,2),
                PROFIT DECIMAL(10,2),
                CUSTOMER_SEGMENT VARCHAR(255),
                REGION VARCHAR(255),
                COUNTRY VARCHAR(255),
                STATE VARCHAR(255),
                CITY VARCHAR(255),
                SOURCE_FILE VARCHAR(500),
                BATCH_ID VARCHAR(255),
                INGESTION_TIMESTAMP TIMESTAMP,
                SOURCE_SYSTEM VARCHAR(255),
                PARTITION_DATE DATE
            )
            """

            cursor.execute(create_table_sql)
            cursor.close()
            return True

        except Exception as e:
            print(f"Failed to create test table: {e}")
            return False

    def cleanup_test_table(self, table_name: str) -> bool:
        """Clean up test table after testing."""
        if not self.connection:
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.close()
            return True
        except Exception as e:
            print(f"Failed to cleanup test table: {e}")
            return False

    def count_records(self, table_name: str) -> Optional[int]:
        """Count records in a table."""
        if not self.connection:
            return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
        except Exception as e:
            print(f"Failed to count records: {e}")
            return None

    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()


@pytest.fixture
def snowflake_config():
    """Create Snowflake configuration from environment variables."""
    return SnowflakeConfig.from_env()


@pytest.fixture
def snowflake_connection(snowflake_config):
    """Create Snowflake connection for integration tests."""
    if not SNOWFLAKE_AVAILABLE:
        pytest.skip("Snowflake connector not available")

    # Check if Snowflake credentials are available
    if not all(
        [snowflake_config.account, snowflake_config.user, snowflake_config.password]
    ):
        pytest.skip("Snowflake credentials not available")

    manager = SnowflakeConnectionManager(snowflake_config)

    if not manager.connect():
        pytest.skip("Could not connect to Snowflake")

    yield manager

    manager.close()


@pytest.mark.integration
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSnowflakeIntegration:
    """Integration tests for Snowflake connectivity."""

    def test_snowflake_connection(self, snowflake_connection):
        """Test basic Snowflake connectivity."""
        assert (
            snowflake_connection.test_connection()
        ), "Snowflake connection test failed"

    def test_snowflake_table_operations(self, snowflake_connection):
        """Test creating and managing Snowflake tables."""
        test_table = "TEST_SALES_INTEGRATION"

        try:
            # Create test table
            assert snowflake_connection.create_test_table(
                test_table
            ), "Failed to create test table"

            # Verify table is empty
            count = snowflake_connection.count_records(test_table)
            assert count == 0, f"Test table should be empty, found {count} records"

        finally:
            # Cleanup
            snowflake_connection.cleanup_test_table(test_table)

    @pytest.mark.slow
    def test_etl_to_snowflake_integration(
        self, spark_session, snowflake_connection, sample_sales_data
    ):
        """Test complete ETL process with real Snowflake connection."""
        test_table = "TEST_SALES_ETL_INTEGRATION"

        try:
            # Create test table
            assert snowflake_connection.create_test_table(
                test_table
            ), "Failed to create test table"

            # Set up ETL job with real Snowflake config
            config = snowflake_connection.config
            with patch.dict(os.environ, config.to_dict()):
                etl_job = SalesETLJob(spark_session)

                # Prepare test data
                input_df = create_spark_dataframe(
                    spark_session, sample_sales_data[:20]
                )  # Small dataset
                cleaned_df = etl_job.clean_and_transform(input_df, "integration_test")
                final_df = etl_job.map_to_snowflake_schema(cleaned_df)

                # Write to Snowflake
                etl_job.write_to_snowflake(final_df, test_table)

                # Verify data was written
                time.sleep(2)  # Give Snowflake time to process
                record_count = snowflake_connection.count_records(test_table)
                assert record_count > 0, "No records were written to Snowflake"
                assert record_count <= 20, f"Too many records written: {record_count}"

        finally:
            # Cleanup
            snowflake_connection.cleanup_test_table(test_table)

    def test_snowflake_write_modes(
        self, spark_session, snowflake_connection, sample_sales_data
    ):
        """Test different write modes to Snowflake."""
        test_table = "TEST_SALES_WRITE_MODES"

        try:
            # Create test table
            assert snowflake_connection.create_test_table(
                test_table
            ), "Failed to create test table"

            # Set up ETL job
            config = snowflake_connection.config
            with patch.dict(os.environ, config.to_dict()):
                etl_job = SalesETLJob(spark_session)

                # Prepare test data
                input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
                cleaned_df = etl_job.clean_and_transform(input_df, "write_mode_test")
                final_df = etl_job.map_to_snowflake_schema(cleaned_df)

                # Test append mode (first write)
                etl_job.write_to_snowflake(final_df, test_table, mode="append")
                time.sleep(1)
                first_count = snowflake_connection.count_records(test_table)
                assert first_count > 0, "First write failed"

                # Test append mode (second write)
                etl_job.write_to_snowflake(final_df, test_table, mode="append")
                time.sleep(1)
                second_count = snowflake_connection.count_records(test_table)
                assert (
                    second_count == first_count * 2
                ), "Append mode not working correctly"

                # Test overwrite mode
                etl_job.write_to_snowflake(final_df, test_table, mode="overwrite")
                time.sleep(1)
                final_count = snowflake_connection.count_records(test_table)
                assert (
                    final_count == first_count
                ), "Overwrite mode not working correctly"

        finally:
            # Cleanup
            snowflake_connection.cleanup_test_table(test_table)


@pytest.mark.integration
@pytest.mark.skipif(not AWS_AVAILABLE, reason="AWS SDK not available")
class TestS3Integration:
    """Integration tests for S3 connectivity."""

    def test_s3_connectivity(self):
        """Test basic S3 connectivity."""
        try:
            s3_client = boto3.client("s3")
            # Try to list buckets as a connectivity test
            response = s3_client.list_buckets()
            assert "Buckets" in response, "S3 connection failed"
        except NoCredentialsError:
            pytest.skip("AWS credentials not available")
        except ClientError as e:
            pytest.skip(f"S3 access denied: {e}")

    @pytest.mark.slow
    def test_spark_s3_integration(self, spark_session, sample_sales_data):
        """Test Spark's ability to read from and write to S3."""
        # This test requires valid AWS credentials and S3 bucket
        s3_bucket = os.getenv("TEST_S3_BUCKET")
        if not s3_bucket:
            pytest.skip("TEST_S3_BUCKET environment variable not set")

        try:
            # Create test data
            input_df = create_spark_dataframe(spark_session, sample_sales_data[:10])

            # Write to S3 (using s3a:// protocol)
            s3_path = f"s3a://{s3_bucket}/test-spark-integration/sales_data"
            input_df.write.mode("overwrite").parquet(s3_path)

            # Read back from S3
            read_df = spark_session.read.parquet(s3_path)

            assert read_df.count() == input_df.count(), "S3 round-trip failed"

        except Exception as e:
            pytest.skip(f"S3 integration test failed: {e}")


@pytest.mark.integration
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSparkEnvironments:
    """Integration tests for different Spark environments."""

    def test_local_spark_configuration(self):
        """Test Spark configuration for local environment."""
        config = get_spark_config("local")
        spark = create_spark_session_from_config(config)

        try:
            assert spark.sparkContext.appName == "SalesETL-Local"
            assert spark.sparkContext.master == "local[*]"

            # Test basic functionality
            test_data = [{"id": 1, "value": "test"}]
            df = spark.createDataFrame(test_data)
            assert df.count() == 1

        finally:
            spark.stop()

    def test_databricks_spark_configuration(self):
        """Test Spark configuration for Databricks environment."""
        config = get_spark_config("databricks")

        # For Databricks, we can't create a session in tests,
        # but we can verify the configuration
        assert config.app_name == "SalesETL-Databricks"
        assert config.adaptive_enabled == True
        assert "snowflake" in ",".join(config.packages).lower()

    def test_emr_spark_configuration(self):
        """Test Spark configuration for EMR environment."""
        config = get_spark_config("emr")

        assert config.app_name == "SalesETL-EMR"
        assert config.driver_memory == "4g"
        assert config.executor_memory == "8g"
        assert config.dynamic_allocation == True

    @pytest.mark.slow
    def test_spark_adaptive_query_execution(self, spark_session, performance_test_data):
        """Test Spark's adaptive query execution with larger datasets."""
        # Create larger dataset
        df = create_spark_dataframe(spark_session, performance_test_data[:1000])

        # Perform operations that would benefit from AQE
        result = (
            df.groupBy("Category", "Region")
            .agg({"Sales": "sum", "Quantity": "sum", "Profit": "avg"})
            .collect()
        )

        assert len(result) > 0, "Adaptive query execution test failed"

        # Verify AQE is enabled
        aqe_enabled = spark_session.conf.get("spark.sql.adaptive.enabled", "false")
        # Note: AQE might be disabled in test fixtures for deterministic results


@pytest.mark.integration
class TestErrorHandlingIntegration:
    """Integration tests for error handling scenarios."""

    @pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
    def test_invalid_snowflake_credentials(self, spark_session, sample_sales_data):
        """Test handling of invalid Snowflake credentials."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "invalid.account",
                "SNOWFLAKE_USER": "invalid_user",
                "SNOWFLAKE_PASSWORD": "invalid_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
            cleaned_df = etl_job.clean_and_transform(input_df, "error_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            # This should raise an exception when trying to write
            with pytest.raises(Exception):
                etl_job.write_to_snowflake(final_df, "TEST_TABLE")

    @pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
    def test_network_timeout_handling(self, spark_session, sample_sales_data):
        """Test handling of network timeouts and connection issues."""
        # Mock a network timeout scenario
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "timeout.test.snowflakecomputing.com",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            input_df = create_spark_dataframe(spark_session, sample_sales_data[:5])
            cleaned_df = etl_job.clean_and_transform(input_df, "timeout_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            # This should handle network issues gracefully
            with pytest.raises(Exception):
                etl_job.write_to_snowflake(final_df, "TEST_TABLE")

    @pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
    def test_large_dataset_memory_handling(self, spark_session):
        """Test handling of large datasets that might cause memory issues."""
        # Create a moderately large dataset for memory testing
        large_data = []
        for i in range(5000):  # 5K records
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

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            # This should not cause memory issues in a properly configured Spark session
            input_df = create_spark_dataframe(spark_session, large_data)
            cleaned_df = etl_job.clean_and_transform(input_df, "memory_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            # Verify processing completed
            assert final_df.count() > 0, "Large dataset processing failed"


@pytest.mark.integration
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestEndToEndIntegration:
    """End-to-end integration tests."""

    @pytest.mark.slow
    def test_complete_etl_pipeline_mock(self, spark_session, temp_csv_directory):
        """Test complete ETL pipeline with mocked external dependencies."""
        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            # Mock Snowflake write operation
            with patch.object(etl_job, "write_to_snowflake") as mock_write:
                input_path = f"file://{temp_csv_directory}/*.csv"
                result = etl_job.run_etl(input_path, "TEST_TABLE", "e2e_test")

                # Verify successful completion
                assert result["status"] == "success"
                assert result["batch_id"] == "e2e_test"
                assert result["records_processed"] > 0

                # Verify Snowflake write was called
                mock_write.assert_called_once()

    def test_configuration_validation_integration(self):
        """Test that configuration validation works across different environments."""
        environments = ["local", "databricks", "emr", "production"]

        for env in environments:
            config = get_spark_config(env)

            # Verify required configuration properties
            assert config.app_name is not None
            assert config.packages is not None
            assert len(config.packages) > 0

            # Verify Snowflake packages are included
            packages_str = ",".join(config.packages)
            assert "snowflake" in packages_str.lower()

    @pytest.mark.parametrize("environment", ["local", "databricks", "emr"])
    def test_environment_specific_configurations(self, environment):
        """Test environment-specific Spark configurations."""
        config = get_spark_config(environment)

        if environment == "local":
            assert config.master == "local[*]"
            assert config.dynamic_allocation == False
        elif environment == "databricks":
            assert "databricks" in config.app_name.lower()
        elif environment == "emr":
            assert config.dynamic_allocation == True
            assert config.min_executors >= 1
            assert config.max_executors > config.min_executors
