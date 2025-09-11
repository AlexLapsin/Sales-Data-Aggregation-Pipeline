#!/usr/bin/env python3
"""
Configuration Validation Tests

This module provides comprehensive tests for validating environment configuration,
.env file setup, and system prerequisites for the sales data pipeline.
"""

import os
import re
import boto3
import psycopg2
import pytest
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any
import logging
from urllib.parse import urlparse
import snowflake.connector
from databricks import sql as databricks_sql
from kafka import KafkaProducer
import json


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigValidator:
    """Validates environment configuration and prerequisites"""

    def __init__(self, env_file_path: str = ".env"):
        self.env_file_path = env_file_path
        self.config = {}
        self.load_configuration()

    def load_configuration(self) -> None:
        """Load configuration from .env file"""
        if os.path.exists(self.env_file_path):
            load_dotenv(self.env_file_path)
            logger.info(f"Loaded configuration from {self.env_file_path}")
        else:
            logger.warning(f"Environment file {self.env_file_path} not found")

    def validate_required_vars(self, required_vars: List[str]) -> Dict[str, bool]:
        """Validate that required environment variables are set"""
        results = {}
        for var in required_vars:
            value = os.getenv(var)
            results[var] = value is not None and value.strip() != ""
            if not results[var]:
                logger.error(f"Required environment variable {var} is missing or empty")
        return results

    def validate_aws_credentials(self) -> bool:
        """Validate AWS credentials and permissions"""
        try:
            # Check AWS credentials are set
            required_aws_vars = [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_DEFAULT_REGION",
            ]
            aws_config = self.validate_required_vars(required_aws_vars)

            if not all(aws_config.values()):
                return False

            # Test AWS credentials by listing S3 buckets
            session = boto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_DEFAULT_REGION"),
            )

            s3_client = session.client("s3")
            s3_client.list_buckets()

            logger.info("AWS credentials validation successful")
            return True

        except Exception as e:
            logger.error(f"AWS credentials validation failed: {e}")
            return False

    def validate_s3_buckets(self) -> Dict[str, bool]:
        """Validate S3 bucket accessibility"""
        results = {}

        try:
            s3_client = boto3.client("s3")

            # Check raw bucket
            raw_bucket = os.getenv("S3_BUCKET")
            if raw_bucket:
                try:
                    s3_client.head_bucket(Bucket=raw_bucket)
                    results["S3_BUCKET"] = True
                    logger.info(f"Raw bucket {raw_bucket} is accessible")
                except Exception as e:
                    results["S3_BUCKET"] = False
                    logger.error(f"Raw bucket {raw_bucket} not accessible: {e}")
            else:
                results["S3_BUCKET"] = False

            # Check processed bucket
            processed_bucket = os.getenv("PROCESSED_BUCKET")
            if processed_bucket:
                try:
                    s3_client.head_bucket(Bucket=processed_bucket)
                    results["PROCESSED_BUCKET"] = True
                    logger.info(f"Processed bucket {processed_bucket} is accessible")
                except Exception as e:
                    results["PROCESSED_BUCKET"] = False
                    logger.error(
                        f"Processed bucket {processed_bucket} not accessible: {e}"
                    )
            else:
                results["PROCESSED_BUCKET"] = False

        except Exception as e:
            logger.error(f"S3 validation failed: {e}")
            results["S3_BUCKET"] = False
            results["PROCESSED_BUCKET"] = False

        return results

    def validate_rds_connection(self) -> bool:
        """Validate RDS database connectivity"""
        try:
            required_rds_vars = ["RDS_HOST", "RDS_USER", "RDS_PASS", "RDS_DB"]
            rds_config = self.validate_required_vars(required_rds_vars)

            if not all(rds_config.values()):
                return False

            # Test database connection
            conn = psycopg2.connect(
                host=os.getenv("RDS_HOST"),
                port=os.getenv("RDS_PORT", "5432"),
                database=os.getenv("RDS_DB"),
                user=os.getenv("RDS_USER"),
                password=os.getenv("RDS_PASS"),
                connect_timeout=10,
            )
            conn.close()

            logger.info("RDS connection validation successful")
            return True

        except Exception as e:
            logger.error(f"RDS connection validation failed: {e}")
            return False

    def validate_snowflake_connection(self) -> bool:
        """Validate Snowflake connectivity (if configured)"""
        try:
            snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
            if not snowflake_account:
                logger.info("Snowflake not configured, skipping validation")
                return True

            required_snowflake_vars = [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER",
                "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE",
            ]
            snowflake_config = self.validate_required_vars(required_snowflake_vars)

            if not all(snowflake_config.values()):
                return False

            # Test Snowflake connection
            conn = snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            )
            conn.close()

            logger.info("Snowflake connection validation successful")
            return True

        except Exception as e:
            logger.error(f"Snowflake connection validation failed: {e}")
            return False

    def validate_databricks_connection(self) -> bool:
        """Validate Databricks connectivity (if configured)"""
        try:
            databricks_host = os.getenv("DATABRICKS_HOST")
            if not databricks_host:
                logger.info("Databricks not configured, skipping validation")
                return True

            required_databricks_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
            databricks_config = self.validate_required_vars(required_databricks_vars)

            if not all(databricks_config.values()):
                return False

            # Test Databricks connection
            with databricks_sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST").replace("https://", ""),
                http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_CLUSTER_ID', 'default')}",
                access_token=os.getenv("DATABRICKS_TOKEN"),
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()

            logger.info("Databricks connection validation successful")
            return True

        except Exception as e:
            logger.error(f"Databricks connection validation failed: {e}")
            return False

    def validate_kafka_config(self) -> bool:
        """Validate Kafka configuration"""
        try:
            kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

            # Test Kafka connection
            producer = KafkaProducer(
                bootstrap_servers=kafka_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
            )
            producer.close()

            logger.info("Kafka configuration validation successful")
            return True

        except Exception as e:
            logger.error(f"Kafka configuration validation failed: {e}")
            return False

    def validate_file_paths(self) -> Dict[str, bool]:
        """Validate required file paths exist"""
        results = {}

        # Check data directory
        host_data_dir = os.getenv("HOST_DATA_DIR")
        if host_data_dir:
            results["HOST_DATA_DIR"] = os.path.exists(host_data_dir)
            if not results["HOST_DATA_DIR"]:
                logger.error(f"Data directory {host_data_dir} does not exist")
        else:
            results["HOST_DATA_DIR"] = False
            logger.error("HOST_DATA_DIR not configured")

        # Check for raw CSV files
        if host_data_dir and os.path.exists(host_data_dir):
            raw_dir = os.path.join(host_data_dir, "raw")
            results["raw_data_dir"] = os.path.exists(raw_dir)

            if results["raw_data_dir"]:
                csv_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
                results["csv_files_exist"] = len(csv_files) > 0
                if not results["csv_files_exist"]:
                    logger.error(f"No CSV files found in {raw_dir}")
            else:
                results["csv_files_exist"] = False
                logger.error(f"Raw data directory {raw_dir} does not exist")

        return results

    def validate_terraform_vars(self) -> Dict[str, bool]:
        """Validate Terraform-specific variables"""
        terraform_vars = [
            "PROJECT_NAME",
            "ENVIRONMENT",
            "ALLOWED_CIDR",
            "DB_INSTANCE_CLASS",
            "DB_ALLOCATED_STORAGE",
        ]

        results = self.validate_required_vars(terraform_vars)

        # Validate CIDR format
        allowed_cidr = os.getenv("ALLOWED_CIDR")
        if allowed_cidr:
            cidr_pattern = r"^(\d{1,3}\.){3}\d{1,3}/\d{1,2}$"
            results["ALLOWED_CIDR_format"] = bool(re.match(cidr_pattern, allowed_cidr))
            if not results["ALLOWED_CIDR_format"]:
                logger.error(f"Invalid CIDR format: {allowed_cidr}")

        return results


# Test fixtures
@pytest.fixture(scope="session")
def config_validator():
    """Fixture providing configuration validator instance"""
    return ConfigValidator()


# Test cases
def test_env_file_exists():
    """Test that .env file exists"""
    assert os.path.exists(
        ".env"
    ), ".env file not found. Copy .env.example to .env and configure it."


def test_core_environment_variables(config_validator):
    """Test that core environment variables are configured"""
    core_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_DEFAULT_REGION",
        "S3_BUCKET",
        "PROCESSED_BUCKET",
        "HOST_DATA_DIR",
        "PIPELINE_IMAGE",
    ]

    results = config_validator.validate_required_vars(core_vars)

    for var, is_valid in results.items():
        assert is_valid, f"Required environment variable {var} is missing or empty"


def test_aws_credentials_valid(config_validator):
    """Test that AWS credentials are valid and have necessary permissions"""
    assert (
        config_validator.validate_aws_credentials()
    ), "AWS credentials validation failed"


def test_s3_buckets_accessible(config_validator):
    """Test that S3 buckets are accessible"""
    results = config_validator.validate_s3_buckets()

    for bucket, is_accessible in results.items():
        assert is_accessible, f"S3 bucket {bucket} is not accessible"


def test_rds_connection_valid(config_validator):
    """Test that RDS database is accessible"""
    assert (
        config_validator.validate_rds_connection()
    ), "RDS connection validation failed"


def test_snowflake_connection_valid(config_validator):
    """Test that Snowflake connection is valid (if configured)"""
    assert (
        config_validator.validate_snowflake_connection()
    ), "Snowflake connection validation failed"


def test_databricks_connection_valid(config_validator):
    """Test that Databricks connection is valid (if configured)"""
    assert (
        config_validator.validate_databricks_connection()
    ), "Databricks connection validation failed"


def test_kafka_configuration_valid(config_validator):
    """Test that Kafka configuration is valid"""
    assert (
        config_validator.validate_kafka_config()
    ), "Kafka configuration validation failed"


def test_required_file_paths_exist(config_validator):
    """Test that required file paths exist"""
    results = config_validator.validate_file_paths()

    for path_type, exists in results.items():
        assert exists, f"Required path {path_type} does not exist or is not configured"


def test_terraform_variables_valid(config_validator):
    """Test that Terraform variables are properly configured"""
    results = config_validator.validate_terraform_vars()

    for var, is_valid in results.items():
        assert is_valid, f"Terraform variable validation failed for {var}"


def test_docker_image_exists():
    """Test that the Docker image is built and available"""
    pipeline_image = os.getenv("PIPELINE_IMAGE", "sales-pipeline:latest")

    try:
        import docker

        client = docker.from_env()
        client.images.get(pipeline_image)
        logger.info(f"Docker image {pipeline_image} found")
    except Exception as e:
        pytest.fail(f"Docker image {pipeline_image} not found: {e}")


def test_environment_consistency():
    """Test that environment variables are consistent with each other"""
    # Test bucket names are different
    s3_bucket = os.getenv("S3_BUCKET")
    processed_bucket = os.getenv("PROCESSED_BUCKET")

    if s3_bucket and processed_bucket:
        assert (
            s3_bucket != processed_bucket
        ), "S3_BUCKET and PROCESSED_BUCKET should be different"

    # Test sales threshold is a valid number
    sales_threshold = os.getenv("SALES_THRESHOLD")
    if sales_threshold:
        try:
            threshold_value = float(sales_threshold)
            assert threshold_value > 0, "SALES_THRESHOLD should be a positive number"
        except ValueError:
            pytest.fail("SALES_THRESHOLD should be a valid number")


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
