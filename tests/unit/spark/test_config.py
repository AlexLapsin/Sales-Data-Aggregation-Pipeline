"""
Configuration and environment-specific tests for Spark ETL job.

This module contains tests for configuration management, environment-specific
settings, and compatibility across different deployment scenarios.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, Optional

# PySpark imports with error handling
try:
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    SparkConf = None

# Local imports
import sys

sys.path.append(os.path.dirname(__file__))

if SPARK_AVAILABLE:
    from src.spark.config import (
        SparkConfig,
        SnowflakeConfig,
        get_spark_config,
        create_spark_session_from_config,
        ENVIRONMENT_CONFIGS,
    )
    from src.spark.jobs.batch_etl import SalesETLJob, create_spark_session


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSparkConfiguration:
    """Test Spark configuration management."""

    def test_default_spark_config(self):
        """Test default SparkConfig initialization."""
        config = SparkConfig()

        assert config.app_name == "SalesETL"
        assert config.adaptive_enabled == True
        assert config.serializer == "org.apache.spark.serializer.KryoSerializer"
        assert config.packages is not None
        assert len(config.packages) > 0

        # Check that Snowflake packages are included
        packages_str = ",".join(config.packages)
        assert "snowflake" in packages_str.lower()

    def test_spark_config_post_init(self):
        """Test SparkConfig __post_init__ method."""
        # Test with None packages (should set default)
        config = SparkConfig(packages=None)
        assert config.packages is not None
        assert len(config.packages) > 0

        # Test with custom packages
        custom_packages = ["custom.package:1.0.0"]
        config = SparkConfig(packages=custom_packages)
        assert config.packages == custom_packages

    def test_spark_config_customization(self):
        """Test customizing SparkConfig parameters."""
        custom_config = SparkConfig(
            app_name="CustomETL",
            master="local[4]",
            driver_memory="8g",
            executor_memory="16g",
            executor_cores=4,
            dynamic_allocation=False,
            packages=["custom:package:1.0"],
        )

        assert custom_config.app_name == "CustomETL"
        assert custom_config.master == "local[4]"
        assert custom_config.driver_memory == "8g"
        assert custom_config.executor_memory == "16g"
        assert custom_config.executor_cores == 4
        assert custom_config.dynamic_allocation == False
        assert custom_config.packages == ["custom:package:1.0"]

    @pytest.mark.parametrize(
        "environment", ["local", "databricks", "emr", "production"]
    )
    def test_environment_specific_configs(self, environment):
        """Test environment-specific Spark configurations."""
        config = get_spark_config(environment)

        assert isinstance(config, SparkConfig)
        assert config.app_name is not None
        assert (
            environment.lower() in config.app_name.lower()
            or environment == "production"
        )

        # Test environment-specific properties
        if environment == "local":
            assert config.master == "local[*]"
            assert config.dynamic_allocation == False
            assert config.driver_memory == "1g"
            assert config.executor_memory == "2g"
        elif environment == "databricks":
            assert "databricks" in config.app_name.lower()
            assert config.adaptive_enabled == True
        elif environment == "emr":
            assert config.dynamic_allocation == True
            assert config.min_executors >= 1
            assert config.max_executors > config.min_executors
            assert config.driver_memory == "4g"
            assert config.executor_memory == "8g"
        elif environment == "production":
            assert config.driver_memory == "4g"
            assert config.executor_memory == "8g"

    def test_invalid_environment_config(self):
        """Test handling of invalid environment names."""
        # Should return default/production config for unknown environments
        config = get_spark_config("unknown_environment")

        assert isinstance(config, SparkConfig)
        assert config.app_name == "SalesETL-Production"

    def test_environment_configs_dictionary(self):
        """Test ENVIRONMENT_CONFIGS dictionary."""
        expected_environments = ["local", "databricks", "emr", "production"]

        for env in expected_environments:
            assert env in ENVIRONMENT_CONFIGS
            assert isinstance(ENVIRONMENT_CONFIGS[env], SparkConfig)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSnowflakeConfiguration:
    """Test Snowflake configuration management."""

    def test_snowflake_config_initialization(self):
        """Test SnowflakeConfig initialization."""
        config = SnowflakeConfig(
            account="test.account", user="test_user", password="test_password"
        )

        assert config.account == "test.account"
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.database == "SALES_DW"  # Default value
        assert config.schema == "RAW"  # Default value
        assert config.warehouse == "ETL_WH"  # Default value
        assert config.role == "SYSADMIN"  # Default value

    def test_snowflake_config_custom_values(self):
        """Test SnowflakeConfig with custom values."""
        config = SnowflakeConfig(
            account="custom.account",
            user="custom_user",
            password="custom_password",
            database="CUSTOM_DB",
            schema="CUSTOM_SCHEMA",
            warehouse="CUSTOM_WH",
            role="CUSTOM_ROLE",
        )

        assert config.account == "custom.account"
        assert config.database == "CUSTOM_DB"
        assert config.schema == "CUSTOM_SCHEMA"
        assert config.warehouse == "CUSTOM_WH"
        assert config.role == "CUSTOM_ROLE"

    def test_snowflake_config_from_env(self):
        """Test SnowflakeConfig.from_env() method."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "env.test.account",
            "SNOWFLAKE_USER": "env_test_user",
            "SNOWFLAKE_PASSWORD": "env_test_password",
            "SNOWFLAKE_DATABASE": "ENV_DB",
            "SNOWFLAKE_SCHEMA": "ENV_SCHEMA",
            "SNOWFLAKE_WAREHOUSE": "ENV_WH",
            "SNOWFLAKE_ROLE": "ENV_ROLE",
        }

        with patch.dict(os.environ, env_vars):
            config = SnowflakeConfig.from_env()

            assert config.account == "env.test.account"
            assert config.user == "env_test_user"
            assert config.password == "env_test_password"
            assert config.database == "ENV_DB"
            assert config.schema == "ENV_SCHEMA"
            assert config.warehouse == "ENV_WH"
            assert config.role == "ENV_ROLE"

    def test_snowflake_config_from_env_defaults(self):
        """Test SnowflakeConfig.from_env() with default values."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "test.account",
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_password",
            # Other values should use defaults
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeConfig.from_env()

            assert config.account == "test.account"
            assert config.user == "test_user"
            assert config.password == "test_password"
            assert config.database == "SALES_DW"  # Default
            assert config.schema == "RAW"  # Default
            assert config.warehouse == "ETL_WH"  # Default
            assert config.role == "SYSADMIN"  # Default

    def test_snowflake_config_to_dict(self):
        """Test SnowflakeConfig.to_dict() method."""
        config = SnowflakeConfig(
            account="test.account",
            user="test_user",
            password="test_password",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            warehouse="TEST_WH",
            role="TEST_ROLE",
        )

        config_dict = config.to_dict()

        expected_dict = {
            "sfUrl": "test.account",
            "sfUser": "test_user",
            "sfPassword": "test_password",
            "sfDatabase": "TEST_DB",
            "sfSchema": "TEST_SCHEMA",
            "sfWarehouse": "TEST_WH",
            "sfRole": "TEST_ROLE",
        }

        assert config_dict == expected_dict


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSparkSessionCreation:
    """Test Spark session creation from configuration."""

    def test_create_spark_session_from_config_local(self):
        """Test creating Spark session from local config."""
        config = get_spark_config("local")
        spark = create_spark_session_from_config(config)

        try:
            assert spark is not None
            assert spark.sparkContext.appName == "SalesETL-Local"
            assert spark.sparkContext.master == "local[*]"

            # Check configuration values
            assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
            assert spark.conf.get("spark.driver.memory") == "1g"
            assert spark.conf.get("spark.executor.memory") == "2g"

        finally:
            spark.stop()

    def test_create_spark_session_with_aws_config(self):
        """Test Spark session creation with AWS configuration."""
        config = get_spark_config("local")

        aws_env = {
            "AWS_ACCESS_KEY_ID": "test_access_key",
            "AWS_SECRET_ACCESS_KEY": "test_secret_key",
        }

        with patch.dict(os.environ, aws_env):
            spark = create_spark_session_from_config(config)

            try:
                # Check that AWS configuration is set
                s3a_access_key = spark.conf.get("spark.hadoop.fs.s3a.access.key")
                s3a_secret_key = spark.conf.get("spark.hadoop.fs.s3a.secret.key")

                assert s3a_access_key == "test_access_key"
                assert s3a_secret_key == "test_secret_key"

                # Check S3A implementation
                s3a_impl = spark.conf.get("spark.hadoop.fs.s3a.impl")
                assert s3a_impl == "org.apache.hadoop.fs.s3a.S3AFileSystem"

            finally:
                spark.stop()

    def test_create_spark_session_without_aws_config(self):
        """Test Spark session creation without AWS configuration."""
        config = get_spark_config("local")

        # Clear AWS environment variables
        with patch.dict(os.environ, {}, clear=True):
            spark = create_spark_session_from_config(config)

            try:
                # AWS configuration should not be set
                try:
                    spark.conf.get("spark.hadoop.fs.s3a.access.key")
                    assert False, "AWS configuration should not be set"
                except Exception:
                    pass  # Expected - configuration not set

            finally:
                spark.stop()

    def test_create_spark_session_packages(self):
        """Test that required packages are configured correctly."""
        config = get_spark_config("local")
        spark = create_spark_session_from_config(config)

        try:
            packages = spark.conf.get("spark.jars.packages")

            # Check for required packages
            assert "snowflake-jdbc" in packages
            assert "spark-snowflake" in packages
            assert "hadoop-aws" in packages

        finally:
            spark.stop()

    def test_spark_session_dynamic_allocation(self):
        """Test Spark session with dynamic allocation settings."""
        config = get_spark_config("emr")  # EMR config has dynamic allocation enabled
        spark = create_spark_session_from_config(config)

        try:
            # Check dynamic allocation settings
            dynamic_enabled = spark.conf.get("spark.dynamicAllocation.enabled")
            min_executors = spark.conf.get("spark.dynamicAllocation.minExecutors")
            max_executors = spark.conf.get("spark.dynamicAllocation.maxExecutors")

            assert dynamic_enabled == "true"
            assert int(min_executors) == config.min_executors
            assert int(max_executors) == config.max_executors

        finally:
            spark.stop()


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestETLJobConfiguration:
    """Test ETL job configuration and initialization."""

    def test_etl_job_configuration_validation(self):
        """Test ETL job configuration validation."""
        # Test valid configuration
        valid_env = {
            "SNOWFLAKE_ACCOUNT": "valid.account",
            "SNOWFLAKE_USER": "valid_user",
            "SNOWFLAKE_PASSWORD": "valid_password",
        }

        with patch.dict(os.environ, valid_env):
            spark = create_spark_session()
            try:
                job = SalesETLJob(spark)
                assert job.snowflake_options["sfUrl"] == "valid.account"
                assert job.snowflake_options["sfUser"] == "valid_user"
                assert job.snowflake_options["sfPassword"] == "valid_password"
            finally:
                spark.stop()

    def test_etl_job_default_snowflake_values(self):
        """Test ETL job with default Snowflake configuration values."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "test.account",
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_password"
            # Other values should use defaults
        }

        with patch.dict(os.environ, env_vars):
            spark = create_spark_session()
            try:
                job = SalesETLJob(spark)

                assert job.snowflake_options["sfDatabase"] == "SALES_DW"
                assert job.snowflake_options["sfSchema"] == "RAW"
                assert job.snowflake_options["sfWarehouse"] == "ETL_WH"
                assert job.snowflake_options["sfRole"] == "SYSADMIN"
            finally:
                spark.stop()

    def test_etl_job_custom_snowflake_values(self):
        """Test ETL job with custom Snowflake configuration values."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "custom.account",
            "SNOWFLAKE_USER": "custom_user",
            "SNOWFLAKE_PASSWORD": "custom_password",
            "SNOWFLAKE_DATABASE": "CUSTOM_DW",
            "SNOWFLAKE_SCHEMA": "CUSTOM_SCHEMA",
            "SNOWFLAKE_WAREHOUSE": "CUSTOM_WH",
            "SNOWFLAKE_ROLE": "CUSTOM_ROLE",
        }

        with patch.dict(os.environ, env_vars):
            spark = create_spark_session()
            try:
                job = SalesETLJob(spark)

                assert job.snowflake_options["sfDatabase"] == "CUSTOM_DW"
                assert job.snowflake_options["sfSchema"] == "CUSTOM_SCHEMA"
                assert job.snowflake_options["sfWarehouse"] == "CUSTOM_WH"
                assert job.snowflake_options["sfRole"] == "CUSTOM_ROLE"
            finally:
                spark.stop()


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestEnvironmentCompatibility:
    """Test compatibility across different deployment environments."""

    def test_local_environment_compatibility(self):
        """Test configuration compatibility for local development."""
        config = get_spark_config("local")

        # Local config should be suitable for development
        assert config.master == "local[*]"
        assert config.dynamic_allocation == False  # Simpler for local testing
        assert config.driver_memory == "1g"  # Conservative memory usage
        assert config.executor_memory == "2g"

        # Should be able to create Spark session
        spark = create_spark_session_from_config(config)
        try:
            # Basic functionality should work
            test_data = [{"id": 1, "value": "test"}]
            df = spark.createDataFrame(test_data)
            assert df.count() == 1
        finally:
            spark.stop()

    def test_databricks_environment_compatibility(self):
        """Test configuration compatibility for Databricks."""
        config = get_spark_config("databricks")

        # Databricks config should be optimized for cloud environment
        assert "databricks" in config.app_name.lower()
        assert config.adaptive_enabled == True

        # Should have appropriate packages for Databricks
        packages_str = ",".join(config.packages)
        assert "snowflake" in packages_str.lower()

        # Note: Can't actually create Spark session in Databricks from tests
        # but we can verify the configuration makes sense

    def test_emr_environment_compatibility(self):
        """Test configuration compatibility for AWS EMR."""
        config = get_spark_config("emr")

        # EMR config should be optimized for AWS cluster environment
        assert config.dynamic_allocation == True
        assert config.min_executors >= 1
        assert config.max_executors > config.min_executors
        assert config.driver_memory == "4g"
        assert config.executor_memory == "8g"

        # Should have AWS-compatible packages
        packages_str = ",".join(config.packages)
        assert "hadoop-aws" in packages_str
        assert "aws-java-sdk" in packages_str

    def test_production_environment_compatibility(self):
        """Test configuration compatibility for production."""
        config = get_spark_config("production")

        # Production config should be robust and performant
        assert config.driver_memory == "4g"
        assert config.executor_memory == "8g"
        assert config.executor_cores == 4
        assert config.adaptive_enabled == True

        # Should have all necessary packages
        packages_str = ",".join(config.packages)
        assert "snowflake" in packages_str.lower()

    @pytest.mark.parametrize(
        "environment", ["local", "databricks", "emr", "production"]
    )
    def test_configuration_consistency(self, environment):
        """Test that configurations are consistent and valid."""
        config = get_spark_config(environment)

        # All configurations should have required properties
        assert config.app_name is not None
        assert len(config.app_name) > 0
        assert config.packages is not None
        assert len(config.packages) > 0
        assert config.serializer is not None

        # Memory settings should be valid
        assert config.driver_memory.endswith("g") or config.driver_memory.endswith("m")
        assert config.executor_memory.endswith("g") or config.executor_memory.endswith(
            "m"
        )
        assert config.executor_cores > 0

        # Dynamic allocation settings should be consistent
        if config.dynamic_allocation:
            assert config.min_executors >= 0
            assert config.max_executors > config.min_executors

    def test_cross_environment_package_compatibility(self):
        """Test that package versions are compatible across environments."""
        environments = ["local", "databricks", "emr", "production"]

        # Collect all package configurations
        all_packages = {}
        for env in environments:
            config = get_spark_config(env)
            all_packages[env] = config.packages

        # Check that Snowflake packages are present in all environments
        for env, packages in all_packages.items():
            packages_str = ",".join(packages)
            assert (
                "snowflake" in packages_str.lower()
            ), f"Snowflake packages missing in {env}"

        # Check for package version consistency where applicable
        snowflake_versions = {}
        for env, packages in all_packages.items():
            for package in packages:
                if "snowflake-jdbc" in package:
                    version = package.split(":")[-1]
                    snowflake_versions[env] = version

        # All environments should use the same Snowflake JDBC version
        if len(snowflake_versions) > 1:
            versions = set(snowflake_versions.values())
            assert (
                len(versions) == 1
            ), f"Inconsistent Snowflake JDBC versions: {snowflake_versions}"


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestConfigurationSecurityAndValidation:
    """Test security aspects and validation of configuration."""

    def test_sensitive_configuration_handling(self):
        """Test that sensitive configuration is handled properly."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "secure.account",
            "SNOWFLAKE_USER": "secure_user",
            "SNOWFLAKE_PASSWORD": "super_secret_password_123!@#",
        }

        with patch.dict(os.environ, env_vars):
            spark = create_spark_session()
            try:
                job = SalesETLJob(spark)

                # Password should be stored but not logged or exposed unnecessarily
                assert (
                    job.snowflake_options["sfPassword"]
                    == "super_secret_password_123!@#"
                )

                # Verify that the job object doesn't accidentally expose password in string representation
                job_str = str(job)
                assert "super_secret_password" not in job_str.lower()

            finally:
                spark.stop()

    def test_configuration_validation_edge_cases(self):
        """Test configuration validation for edge cases."""
        # Test with empty strings (should be treated as missing)
        invalid_env = {
            "SNOWFLAKE_ACCOUNT": "",
            "SNOWFLAKE_USER": "valid_user",
            "SNOWFLAKE_PASSWORD": "valid_password",
        }

        with patch.dict(os.environ, invalid_env):
            spark = create_spark_session()
            try:
                with pytest.raises(
                    ValueError, match="Missing required Snowflake configuration"
                ):
                    SalesETLJob(spark)
            finally:
                spark.stop()

    def test_configuration_with_special_characters(self):
        """Test configuration handling with special characters."""
        special_env = {
            "SNOWFLAKE_ACCOUNT": "test-account.region.cloud",
            "SNOWFLAKE_USER": "user@domain.com",
            "SNOWFLAKE_PASSWORD": "P@ssw0rd!#$%^&*()",
            "SNOWFLAKE_DATABASE": "DB_WITH_UNDERSCORES",
            "SNOWFLAKE_SCHEMA": "SCHEMA-WITH-DASHES",
        }

        with patch.dict(os.environ, special_env):
            spark = create_spark_session()
            try:
                job = SalesETLJob(spark)

                # Special characters should be preserved
                assert job.snowflake_options["sfUrl"] == "test-account.region.cloud"
                assert job.snowflake_options["sfUser"] == "user@domain.com"
                assert job.snowflake_options["sfPassword"] == "P@ssw0rd!#$%^&*()"
                assert job.snowflake_options["sfDatabase"] == "DB_WITH_UNDERSCORES"
                assert job.snowflake_options["sfSchema"] == "SCHEMA-WITH-DASHES"

            finally:
                spark.stop()

    def test_configuration_inheritance_and_overrides(self):
        """Test configuration inheritance and environment variable overrides."""
        # Set base configuration
        base_env = {
            "SNOWFLAKE_ACCOUNT": "base.account",
            "SNOWFLAKE_USER": "base_user",
            "SNOWFLAKE_PASSWORD": "base_password",
            "SNOWFLAKE_DATABASE": "BASE_DB",
        }

        with patch.dict(os.environ, base_env):
            config1 = SnowflakeConfig.from_env()

            # Override some values
            override_env = base_env.copy()
            override_env.update(
                {
                    "SNOWFLAKE_DATABASE": "OVERRIDE_DB",
                    "SNOWFLAKE_SCHEMA": "OVERRIDE_SCHEMA",
                }
            )

            with patch.dict(os.environ, override_env):
                config2 = SnowflakeConfig.from_env()

                # Base values should remain the same
                assert config1.account == config2.account
                assert config1.user == config2.user
                assert config1.password == config2.password

                # Overridden values should be different
                assert config1.database == "BASE_DB"
                assert config2.database == "OVERRIDE_DB"
                assert config1.schema == "RAW"  # Default
                assert config2.schema == "OVERRIDE_SCHEMA"
