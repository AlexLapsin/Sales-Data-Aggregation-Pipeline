"""
Spark Configuration for Sales ETL Jobs

Centralized configuration for Spark applications with environment-specific settings.
"""

import os
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class SparkConfig:
    """Spark configuration settings"""

    app_name: str = "SalesETL"
    master: Optional[str] = None

    # Spark SQL settings
    adaptive_enabled: bool = True
    adaptive_coalesce_partitions: bool = True
    adaptive_skew_join: bool = True

    # Memory settings
    driver_memory: str = "2g"
    executor_memory: str = "4g"
    executor_cores: int = 2

    # Dynamic allocation
    dynamic_allocation: bool = True
    min_executors: int = 1
    max_executors: int = 10

    # Serialization
    serializer: str = "org.apache.spark.serializer.KryoSerializer"

    # Packages
    packages: list = None

    def __post_init__(self):
        if self.packages is None:
            self.packages = [
                "net.snowflake:snowflake-jdbc:3.14.3",
                "net.snowflake:spark-snowflake_2.12:2.11.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ]


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""

    account: str
    user: str
    password: str
    database: str = "SALES_DW"
    schema: str = "RAW"
    warehouse: str = "ETL_WH"
    role: str = "SYSADMIN"

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Create configuration from environment variables"""
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        )

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for Spark options"""
        return {
            "sfUrl": self.account,
            "sfUser": self.user,
            "sfPassword": self.password,
            "sfDatabase": self.database,
            "sfSchema": self.schema,
            "sfWarehouse": self.warehouse,
            "sfRole": self.role,
        }


def get_spark_config(environment: str = "local") -> SparkConfig:
    """Get Spark configuration for specific environment"""

    if environment == "local":
        return SparkConfig(
            app_name="SalesETL-Local",
            master="local[*]",
            driver_memory="1g",
            executor_memory="2g",
            dynamic_allocation=False,
            max_executors=2,
        )

    elif environment == "databricks":
        return SparkConfig(
            app_name="SalesETL-Databricks",
            # Databricks manages cluster settings
            adaptive_enabled=True,
            packages=[
                "net.snowflake:snowflake-jdbc:3.14.3",
                "net.snowflake:spark-snowflake_2.12:2.11.3",
            ],
        )

    elif environment == "emr":
        return SparkConfig(
            app_name="SalesETL-EMR",
            driver_memory="4g",
            executor_memory="8g",
            executor_cores=4,
            dynamic_allocation=True,
            min_executors=2,
            max_executors=20,
        )

    else:
        # Default/production config
        return SparkConfig(
            app_name="SalesETL-Production",
            driver_memory="4g",
            executor_memory="8g",
            executor_cores=4,
        )


def create_spark_session_from_config(config: SparkConfig):
    """Create Spark session from configuration"""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(config.app_name)

    # Set master if specified
    if config.master:
        builder = builder.master(config.master)

    # SQL settings
    builder = builder.config(
        "spark.sql.adaptive.enabled", str(config.adaptive_enabled).lower()
    )
    builder = builder.config(
        "spark.sql.adaptive.coalescePartitions.enabled",
        str(config.adaptive_coalesce_partitions).lower(),
    )
    builder = builder.config(
        "spark.sql.adaptive.skewJoin.enabled", str(config.adaptive_skew_join).lower()
    )

    # Memory settings
    builder = builder.config("spark.driver.memory", config.driver_memory)
    builder = builder.config("spark.executor.memory", config.executor_memory)
    builder = builder.config("spark.executor.cores", str(config.executor_cores))

    # Dynamic allocation
    if config.dynamic_allocation:
        builder = builder.config("spark.dynamicAllocation.enabled", "true")
        builder = builder.config(
            "spark.dynamicAllocation.minExecutors", str(config.min_executors)
        )
        builder = builder.config(
            "spark.dynamicAllocation.maxExecutors", str(config.max_executors)
        )

    # Serialization
    builder = builder.config("spark.serializer", config.serializer)

    # Packages
    if config.packages:
        builder = builder.config("spark.jars.packages", ",".join(config.packages))

    # AWS configuration (if needed)
    if os.getenv("AWS_ACCESS_KEY_ID"):
        builder = builder.config(
            "spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        # S3A optimizations
        builder = builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
        builder = builder.config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB
        builder = builder.config("spark.hadoop.fs.s3a.multipart.size", "134217728")

    return builder.getOrCreate()


# Environment-specific configurations
ENVIRONMENT_CONFIGS = {
    "local": get_spark_config("local"),
    "databricks": get_spark_config("databricks"),
    "emr": get_spark_config("emr"),
    "production": get_spark_config("production"),
}
