#!/usr/bin/env python3
"""
End-to-End Integration Test Configuration

This module provides configuration management for comprehensive
integration testing of the entire sales data aggregation pipeline.
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path


@dataclass
class TestDataConfig:
    """Configuration for test data generation"""

    # Sample sizes for different test scenarios
    small_dataset_size: int = 100
    medium_dataset_size: int = 1000
    large_dataset_size: int = 10000
    performance_dataset_size: int = 100000

    # Data generation parameters
    date_range_days: int = 30
    num_regions: int = 5
    num_categories: int = 10
    num_products_per_category: int = 20

    # Test data paths
    test_data_dir: Path = field(
        default_factory=lambda: Path("tests/integration/test_data")
    )
    raw_csv_dir: Path = field(
        default_factory=lambda: Path("tests/integration/test_data/raw")
    )
    expected_output_dir: Path = field(
        default_factory=lambda: Path("tests/integration/test_data/expected")
    )


@dataclass
class InfrastructureConfig:
    """Configuration for test infrastructure components"""

    # Docker containers for testing
    kafka_container_name: str = "test-kafka"
    zookeeper_container_name: str = "test-zookeeper"
    postgres_container_name: str = "test-postgres"
    snowflake_container_name: str = "test-snowflake"  # For Snowflake local testing

    # Network configuration
    test_network_name: str = "sales-pipeline-test-network"
    kafka_port: int = 29092
    postgres_port: int = 15432

    # Test database configuration
    test_db_name: str = "test_sales_pipeline"
    test_db_user: str = "test_user"
    test_db_password: str = "test_password"

    # S3 mock configuration
    minio_port: int = 19000
    test_bucket_name: str = "test-sales-data"
    test_processed_bucket_name: str = "test-processed-sales"


@dataclass
class PipelineStageConfig:
    """Configuration for individual pipeline stage testing"""

    # Stage timeouts (seconds)
    kafka_producer_timeout: int = 60
    spark_job_timeout: int = 300
    dbt_run_timeout: int = 180
    data_validation_timeout: int = 120

    # Quality thresholds
    min_data_quality_score: float = 0.8
    max_duplicate_percentage: float = 0.05
    max_null_percentage: float = 0.1

    # Performance benchmarks (records per second)
    min_kafka_throughput: float = 100.0
    min_spark_throughput: float = 1000.0
    min_dbt_throughput: float = 500.0


@dataclass
class TestEnvironmentConfig:
    """Configuration for test environment setup"""

    # Environment variables for testing
    test_env_vars: Dict[str, str] = field(
        default_factory=lambda: {
            "ENVIRONMENT": "test",
            "LOG_LEVEL": "DEBUG",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
            "KAFKA_TOPIC": "test_sales_events",
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "15432",
            "POSTGRES_DB": "test_sales_pipeline",
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "S3_ENDPOINT_URL": "http://localhost:19000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "S3_BUCKET": "test-sales-data",
            "PROCESSED_BUCKET": "test-processed-sales",
        }
    )

    # Test execution configuration
    parallel_test_workers: int = 4
    test_retry_attempts: int = 3
    test_retry_delay: float = 5.0

    # Cleanup configuration
    cleanup_containers_on_success: bool = True
    cleanup_containers_on_failure: bool = False
    preserve_test_data: bool = False


class E2ETestConfig:
    """Comprehensive end-to-end test configuration manager"""

    def __init__(self, config_file: Optional[str] = None):
        self.logger = logging.getLogger(__name__)

        # Initialize configuration sections
        self.test_data = TestDataConfig()
        self.infrastructure = InfrastructureConfig()
        self.pipeline_stages = PipelineStageConfig()
        self.test_environment = TestEnvironmentConfig()

        # Load custom configuration if provided
        if config_file:
            self.load_config_file(config_file)

        # Override with environment variables
        self.load_from_environment()

        # Validate configuration
        self.validate_config()

    def load_config_file(self, config_file: str) -> None:
        """Load configuration from file"""
        import json
        import yaml

        config_path = Path(config_file)
        if not config_path.exists():
            self.logger.warning(f"Config file not found: {config_file}")
            return

        try:
            if config_path.suffix.lower() in [".yaml", ".yml"]:
                with open(config_path, "r") as f:
                    config_data = yaml.safe_load(f)
            elif config_path.suffix.lower() == ".json":
                with open(config_path, "r") as f:
                    config_data = json.load(f)
            else:
                self.logger.error(f"Unsupported config file format: {config_file}")
                return

            # Update configuration sections
            if "test_data" in config_data:
                for key, value in config_data["test_data"].items():
                    if hasattr(self.test_data, key):
                        setattr(self.test_data, key, value)

            if "infrastructure" in config_data:
                for key, value in config_data["infrastructure"].items():
                    if hasattr(self.infrastructure, key):
                        setattr(self.infrastructure, key, value)

            if "pipeline_stages" in config_data:
                for key, value in config_data["pipeline_stages"].items():
                    if hasattr(self.pipeline_stages, key):
                        setattr(self.pipeline_stages, key, value)

            if "test_environment" in config_data:
                for key, value in config_data["test_environment"].items():
                    if hasattr(self.test_environment, key):
                        setattr(self.test_environment, key, value)

        except Exception as e:
            self.logger.error(f"Failed to load config file {config_file}: {e}")

    def load_from_environment(self) -> None:
        """Load configuration from environment variables"""

        # Test data configuration
        self.test_data.small_dataset_size = int(
            os.getenv("E2E_SMALL_DATASET_SIZE", self.test_data.small_dataset_size)
        )
        self.test_data.medium_dataset_size = int(
            os.getenv("E2E_MEDIUM_DATASET_SIZE", self.test_data.medium_dataset_size)
        )
        self.test_data.large_dataset_size = int(
            os.getenv("E2E_LARGE_DATASET_SIZE", self.test_data.large_dataset_size)
        )

        # Infrastructure configuration
        self.infrastructure.kafka_port = int(
            os.getenv("E2E_KAFKA_PORT", self.infrastructure.kafka_port)
        )
        self.infrastructure.postgres_port = int(
            os.getenv("E2E_POSTGRES_PORT", self.infrastructure.postgres_port)
        )

        # Update test environment variables with actual environment
        for key, default_value in self.test_environment.test_env_vars.items():
            env_value = os.getenv(key, default_value)
            self.test_environment.test_env_vars[key] = env_value

    def validate_config(self) -> None:
        """Validate configuration for consistency and completeness"""

        # Create test directories if they don't exist
        self.test_data.test_data_dir.mkdir(parents=True, exist_ok=True)
        self.test_data.raw_csv_dir.mkdir(parents=True, exist_ok=True)
        self.test_data.expected_output_dir.mkdir(parents=True, exist_ok=True)

        # Validate data size constraints
        if self.test_data.small_dataset_size >= self.test_data.medium_dataset_size:
            raise ValueError("Small dataset size must be less than medium dataset size")

        if self.test_data.medium_dataset_size >= self.test_data.large_dataset_size:
            raise ValueError("Medium dataset size must be less than large dataset size")

        # Validate quality thresholds
        if not (0.0 <= self.pipeline_stages.min_data_quality_score <= 1.0):
            raise ValueError("Data quality score must be between 0.0 and 1.0")

        if not (0.0 <= self.pipeline_stages.max_duplicate_percentage <= 1.0):
            raise ValueError("Duplicate percentage must be between 0.0 and 1.0")

        if not (0.0 <= self.pipeline_stages.max_null_percentage <= 1.0):
            raise ValueError("Null percentage must be between 0.0 and 1.0")

        # Validate port assignments don't conflict
        used_ports = {
            self.infrastructure.kafka_port,
            self.infrastructure.postgres_port,
            self.infrastructure.minio_port,
        }

        if len(used_ports) != 3:
            raise ValueError("Port assignments must be unique")

        self.logger.info("Configuration validation completed successfully")

    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka-specific configuration"""
        return {
            "bootstrap_servers": f"localhost:{self.infrastructure.kafka_port}",
            "topic": self.test_environment.test_env_vars.get(
                "KAFKA_TOPIC", "test_sales_events"
            ),
            "container_name": self.infrastructure.kafka_container_name,
            "zookeeper_container": self.infrastructure.zookeeper_container_name,
            "timeout": self.pipeline_stages.kafka_producer_timeout,
            "min_throughput": self.pipeline_stages.min_kafka_throughput,
        }

    def get_database_config(self) -> Dict[str, Any]:
        """Get database-specific configuration"""
        return {
            "host": "localhost",
            "port": self.infrastructure.postgres_port,
            "database": self.infrastructure.test_db_name,
            "user": self.infrastructure.test_db_user,
            "password": self.infrastructure.test_db_password,
            "container_name": self.infrastructure.postgres_container_name,
        }

    def get_s3_config(self) -> Dict[str, Any]:
        """Get S3/MinIO-specific configuration"""
        return {
            "endpoint_url": f"http://localhost:{self.infrastructure.minio_port}",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "bucket_name": self.infrastructure.test_bucket_name,
            "processed_bucket_name": self.infrastructure.test_processed_bucket_name,
        }

    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark-specific configuration"""
        return {
            "timeout": self.pipeline_stages.spark_job_timeout,
            "min_throughput": self.pipeline_stages.min_spark_throughput,
            "app_name": "E2E_Test_Spark_Job",
        }

    def get_dbt_config(self) -> Dict[str, Any]:
        """Get dbt-specific configuration"""
        return {
            "timeout": self.pipeline_stages.dbt_run_timeout,
            "min_throughput": self.pipeline_stages.min_dbt_throughput,
            "profiles_dir": "tests/integration/dbt_profiles",
        }

    def get_quality_thresholds(self) -> Dict[str, float]:
        """Get data quality thresholds"""
        return {
            "min_data_quality_score": self.pipeline_stages.min_data_quality_score,
            "max_duplicate_percentage": self.pipeline_stages.max_duplicate_percentage,
            "max_null_percentage": self.pipeline_stages.max_null_percentage,
        }

    def get_test_environment_vars(self) -> Dict[str, str]:
        """Get complete test environment variables"""
        return self.test_environment.test_env_vars.copy()

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for serialization"""
        return {
            "test_data": {
                "small_dataset_size": self.test_data.small_dataset_size,
                "medium_dataset_size": self.test_data.medium_dataset_size,
                "large_dataset_size": self.test_data.large_dataset_size,
                "performance_dataset_size": self.test_data.performance_dataset_size,
                "date_range_days": self.test_data.date_range_days,
                "num_regions": self.test_data.num_regions,
                "num_categories": self.test_data.num_categories,
                "num_products_per_category": self.test_data.num_products_per_category,
            },
            "infrastructure": {
                "kafka_port": self.infrastructure.kafka_port,
                "postgres_port": self.infrastructure.postgres_port,
                "minio_port": self.infrastructure.minio_port,
                "test_bucket_name": self.infrastructure.test_bucket_name,
                "test_processed_bucket_name": self.infrastructure.test_processed_bucket_name,
            },
            "pipeline_stages": {
                "kafka_producer_timeout": self.pipeline_stages.kafka_producer_timeout,
                "spark_job_timeout": self.pipeline_stages.spark_job_timeout,
                "dbt_run_timeout": self.pipeline_stages.dbt_run_timeout,
                "min_data_quality_score": self.pipeline_stages.min_data_quality_score,
                "max_duplicate_percentage": self.pipeline_stages.max_duplicate_percentage,
                "max_null_percentage": self.pipeline_stages.max_null_percentage,
            },
            "test_environment": {
                "parallel_test_workers": self.test_environment.parallel_test_workers,
                "test_retry_attempts": self.test_environment.test_retry_attempts,
                "cleanup_containers_on_success": self.test_environment.cleanup_containers_on_success,
                "cleanup_containers_on_failure": self.test_environment.cleanup_containers_on_failure,
            },
        }


# Global configuration instance
E2E_CONFIG = E2ETestConfig()
