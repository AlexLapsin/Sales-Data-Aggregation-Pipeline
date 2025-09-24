#!/usr/bin/env python3
"""
Configuration Validation Framework - "Setup Doctor"
Comprehensive validation for the Sales Data Aggregation Pipeline

This tool validates all configuration files, environment variables, and service
connectivity to help users quickly identify and fix configuration issues.

Usage:
    python config_validator.py --env dev
    python config_validator.py --env prod --check-connectivity
    python config_validator.py --report-only
"""

import os
import sys
import json
import re
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import subprocess
import tempfile
from urllib.parse import urlparse
import ipaddress
import yaml

# Third-party imports for connectivity testing
try:
    import boto3
    import psycopg2
    import snowflake.connector
    from kafka import KafkaProducer, KafkaConsumer
    from databricks_cli.sdk.api_client import ApiClient

    HAS_CLOUD_LIBS = True
except ImportError:
    HAS_CLOUD_LIBS = False

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class Severity(Enum):
    """Validation result severity levels"""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Environment(Enum):
    """Supported environment types"""

    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
    TEST = "test"


@dataclass
class ValidationResult:
    """Single validation result"""

    category: str
    check: str
    severity: Severity
    status: str
    message: str
    suggestions: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report"""

    environment: str
    timestamp: str
    results: List[ValidationResult] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)

    def add_result(self, result: ValidationResult):
        """Add a validation result"""
        self.results.append(result)
        severity_key = result.severity.value.lower()
        self.summary[severity_key] = self.summary.get(severity_key, 0) + 1

    def has_errors(self) -> bool:
        """Check if report has any errors or critical issues"""
        return (self.summary.get("error", 0) + self.summary.get("critical", 0)) > 0

    def has_warnings(self) -> bool:
        """Check if report has any warnings"""
        return self.summary.get("warning", 0) > 0


class EnvironmentVariableValidator:
    """Validates environment variables and their formats"""

    # Required environment variables by category
    REQUIRED_VARS = {
        "aws": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"],
        "storage": ["S3_BUCKET", "PROCESSED_BUCKET"],
        "database": ["RDS_HOST", "RDS_USER", "RDS_PASS", "RDS_DB"],
        "terraform": ["PROJECT_NAME", "ENVIRONMENT", "ALLOWED_CIDR"],
    }

    # Optional environment variables by category
    OPTIONAL_VARS = {
        "aws": ["AWS_PROFILE", "AWS_SESSION_TOKEN"],
        "storage": ["S3_PREFIX"],
        "database": ["RDS_PORT"],
        "snowflake": [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA",
            "SNOWFLAKE_REGION",
        ],
        "databricks": ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLUSTER_ID"],
        "kafka": [
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC",
            "PRODUCER_INTERVAL",
            "PRODUCER_BATCH_SIZE",
            "PRODUCER_COMPRESSION",
        ],
        "pipeline": [
            "HOST_DATA_DIR",
            "HOST_REPO_ROOT",
            "PIPELINE_IMAGE",
            "SALES_THRESHOLD",
        ],
        "airflow": [
            "_AIRFLOW_WWW_USER_USERNAME",
            "_AIRFLOW_WWW_USER_PASSWORD",
            "_AIRFLOW_WWW_USER_EMAIL",
            "OWNER_NAME",
            "ALERT_EMAIL",
        ],
        "terraform": [
            "DB_INSTANCE_CLASS",
            "DB_ALLOCATED_STORAGE",
            "DB_ENGINE_VERSION",
            "DB_BACKUP_RETENTION_DAYS",
            "PUBLICLY_ACCESSIBLE",
            "TRUSTED_PRINCIPAL_ARN",
            "ENABLE_MSK",
            "KAFKA_VERSION",
            "BROKER_INSTANCE_TYPE",
            "NUMBER_OF_BROKERS",
            "ENABLE_SNOWFLAKE_OBJECTS",
        ],
    }

    # Format validation patterns
    FORMAT_PATTERNS = {
        "AWS_DEFAULT_REGION": r"^[a-z0-9\-]+$",
        "AWS_ACCESS_KEY_ID": r"^[A-Z0-9]{20}$",
        "AWS_SECRET_ACCESS_KEY": r"^[A-Za-z0-9/+=]{40}$",
        "S3_BUCKET": r"^[a-z0-9\-\.]+$",
        "RDS_PORT": r"^\d{4,5}$",
        "ALLOWED_CIDR": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$",
        "SNOWFLAKE_ACCOUNT": r"^[a-zA-Z0-9\-\.]+$",
        "DATABRICKS_HOST": r"^https://[a-zA-Z0-9\-\.]+\.databricks\.com/?$",
        "DATABRICKS_TOKEN": r"^dapi[a-f0-9]{32}$",
        "KAFKA_BOOTSTRAP_SERVERS": r"^[a-zA-Z0-9\-\.:,\s]+$",
        "SALES_THRESHOLD": r"^\d+(\.\d+)?$",
        "PIPELINE_IMAGE": r"^[a-zA-Z0-9\-_/.:]+$",
    }

    def validate_environment_variables(
        self, env_vars: Dict[str, str], environment: Environment
    ) -> List[ValidationResult]:
        """Validate all environment variables"""
        results = []

        # Check required variables by environment
        required_categories = self._get_required_categories(environment)

        for category in required_categories:
            if category in self.REQUIRED_VARS:
                for var in self.REQUIRED_VARS[category]:
                    result = self._validate_required_var(var, env_vars, category)
                    results.append(result)

        # Validate formats for present variables
        for var, value in env_vars.items():
            if var in self.FORMAT_PATTERNS and value:
                result = self._validate_format(var, value)
                results.append(result)

        # Check for security issues
        results.extend(self._check_security_issues(env_vars))

        # Check for common misconfigurations
        results.extend(self._check_common_misconfigurations(env_vars))

        return results

    def _get_required_categories(self, environment: Environment) -> List[str]:
        """Get required variable categories by environment"""
        base_categories = ["aws", "storage", "terraform"]

        if environment == Environment.PROD:
            return base_categories + ["database"]
        elif environment == Environment.DEV:
            return base_categories
        else:  # staging, test
            return base_categories + ["database"]

    def _validate_required_var(
        self, var: str, env_vars: Dict[str, str], category: str
    ) -> ValidationResult:
        """Validate a single required variable"""
        value = env_vars.get(var)

        if not value:
            return ValidationResult(
                category=f"environment.{category}",
                check=f"required_var_{var}",
                severity=Severity.ERROR,
                status="MISSING",
                message=f"Required environment variable {var} is not set",
                suggestions=[
                    f"Set {var} in your .env file",
                    f"Check .env.example for the correct format",
                    f"Ensure you've copied .env.example to .env",
                ],
            )

        if (
            value.strip().startswith("YOUR_")
            or value.strip() == "CHANGE_ME_STRONG_PASSWORD"
        ):
            return ValidationResult(
                category=f"environment.{category}",
                check=f"template_value_{var}",
                severity=Severity.ERROR,
                status="TEMPLATE_VALUE",
                message=f"Environment variable {var} contains template value: {value}",
                suggestions=[
                    f"Replace the template value with your actual {category} configuration",
                    "Check the documentation for the correct format",
                ],
            )

        return ValidationResult(
            category=f"environment.{category}",
            check=f"required_var_{var}",
            severity=Severity.INFO,
            status="VALID",
            message=f"Required variable {var} is properly set",
        )

    def _validate_format(self, var: str, value: str) -> ValidationResult:
        """Validate variable format against pattern"""
        pattern = self.FORMAT_PATTERNS[var]

        if not re.match(pattern, value):
            return ValidationResult(
                category="environment.format",
                check=f"format_{var}",
                severity=Severity.ERROR,
                status="INVALID_FORMAT",
                message=f"Environment variable {var} has invalid format: {value}",
                suggestions=[
                    f"Check the format for {var}",
                    f"Expected pattern: {pattern}",
                    "Refer to documentation for examples",
                ],
            )

        return ValidationResult(
            category="environment.format",
            check=f"format_{var}",
            severity=Severity.INFO,
            status="VALID",
            message=f"Variable {var} has correct format",
        )

    def _check_security_issues(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check for common security issues"""
        results = []

        # Check for weak passwords
        password_vars = ["RDS_PASS", "SNOWFLAKE_PASSWORD", "_AIRFLOW_WWW_USER_PASSWORD"]
        for var in password_vars:
            value = env_vars.get(var, "")
            if value and len(value) < 8:
                results.append(
                    ValidationResult(
                        category="security.passwords",
                        check=f"weak_password_{var}",
                        severity=Severity.WARNING,
                        status="WEAK_PASSWORD",
                        message=f"Password for {var} is shorter than 8 characters",
                        suggestions=[
                            "Use a strong password with at least 8 characters",
                            "Include uppercase, lowercase, numbers, and special characters",
                            "Consider using a password manager",
                        ],
                    )
                )

        # Check for default credentials
        if (
            env_vars.get("_AIRFLOW_WWW_USER_USERNAME") == "admin"
            and env_vars.get("_AIRFLOW_WWW_USER_PASSWORD") == "admin"
        ):
            results.append(
                ValidationResult(
                    category="security.credentials",
                    check="default_airflow_creds",
                    severity=Severity.WARNING,
                    status="DEFAULT_CREDENTIALS",
                    message="Using default Airflow credentials (admin/admin)",
                    suggestions=[
                        "Change default Airflow username and password",
                        "Set _AIRFLOW_WWW_USER_USERNAME and _AIRFLOW_WWW_USER_PASSWORD in .env",
                    ],
                )
            )

        # Check for overly permissive CIDR
        cidr = env_vars.get("ALLOWED_CIDR", "")
        if cidr == "0.0.0.0/0":
            results.append(
                ValidationResult(
                    category="security.network",
                    check="overly_permissive_cidr",
                    severity=Severity.CRITICAL,
                    status="SECURITY_RISK",
                    message="ALLOWED_CIDR is set to 0.0.0.0/0 (allows all IPs)",
                    suggestions=[
                        "Restrict ALLOWED_CIDR to your specific IP or subnet",
                        "Use your public IP followed by /32 for single IP access",
                        "Check your public IP with: curl ifconfig.me",
                    ],
                )
            )

        return results

    def _check_common_misconfigurations(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check for common configuration mistakes"""
        results = []

        # Check S3 bucket naming consistency
        s3_bucket = env_vars.get("S3_BUCKET", "")
        processed_bucket = env_vars.get("PROCESSED_BUCKET", "")
        project_name = env_vars.get("PROJECT_NAME", "")

        if s3_bucket and processed_bucket and project_name:
            if not s3_bucket.startswith(
                project_name
            ) or not processed_bucket.startswith(project_name):
                results.append(
                    ValidationResult(
                        category="configuration.consistency",
                        check="bucket_naming_convention",
                        severity=Severity.WARNING,
                        status="INCONSISTENT_NAMING",
                        message="S3 bucket names don't follow project naming convention",
                        suggestions=[
                            f"Consider naming buckets: {project_name}-raw, {project_name}-processed",
                            "Consistent naming helps with organization and IAM policies",
                        ],
                    )
                )

        # Check for Windows path formats in HOST_DATA_DIR
        host_data_dir = env_vars.get("HOST_DATA_DIR", "")
        if (
            host_data_dir
            and "\\" in host_data_dir
            and not host_data_dir.startswith("/")
        ):
            results.append(
                ValidationResult(
                    category="configuration.paths",
                    check="windows_path_format",
                    severity=Severity.WARNING,
                    status="WINDOWS_PATH_FORMAT",
                    message="HOST_DATA_DIR uses Windows path format",
                    suggestions=[
                        "Convert Windows paths to Unix format for Docker compatibility",
                        "Use forward slashes (/) instead of backslashes (\\)",
                        "Example: C:\\Users\\name\\project -> /c/Users/name/project",
                    ],
                )
            )

        return results


class ConnectivityTester:
    """Tests connectivity to external services"""

    def test_aws_connectivity(self, env_vars: Dict[str, str]) -> List[ValidationResult]:
        """Test AWS connectivity and permissions"""
        results = []

        if not HAS_CLOUD_LIBS:
            results.append(
                ValidationResult(
                    category="connectivity.aws",
                    check="aws_libraries",
                    severity=Severity.WARNING,
                    status="LIBRARIES_MISSING",
                    message="boto3 library not available for AWS connectivity testing",
                    suggestions=["Install boto3: pip install boto3"],
                )
            )
            return results

        try:
            # Create boto3 client
            session = boto3.Session(
                aws_access_key_id=env_vars.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=env_vars.get("AWS_SECRET_ACCESS_KEY"),
                region_name=env_vars.get("AWS_DEFAULT_REGION", "us-east-1"),
            )

            # Test STS (basic AWS auth)
            sts_client = session.client("sts")
            identity = sts_client.get_caller_identity()

            results.append(
                ValidationResult(
                    category="connectivity.aws",
                    check="aws_authentication",
                    severity=Severity.INFO,
                    status="SUCCESS",
                    message="AWS authentication successful",
                    details={
                        "account": identity.get("Account"),
                        "user_id": identity.get("UserId"),
                        "arn": identity.get("Arn"),
                    },
                )
            )

            # Test S3 bucket access
            s3_client = session.client("s3")
            bucket_name = env_vars.get("S3_BUCKET")

            if bucket_name:
                try:
                    s3_client.head_bucket(Bucket=bucket_name)
                    results.append(
                        ValidationResult(
                            category="connectivity.aws",
                            check="s3_bucket_access",
                            severity=Severity.INFO,
                            status="SUCCESS",
                            message=f"S3 bucket {bucket_name} is accessible",
                        )
                    )
                except Exception as e:
                    results.append(
                        ValidationResult(
                            category="connectivity.aws",
                            check="s3_bucket_access",
                            severity=Severity.ERROR,
                            status="ACCESS_DENIED",
                            message=f"Cannot access S3 bucket {bucket_name}: {str(e)}",
                            suggestions=[
                                "Check if the bucket exists",
                                "Verify IAM permissions for S3 access",
                                "Ensure bucket name is correct in .env file",
                            ],
                        )
                    )

        except Exception as e:
            results.append(
                ValidationResult(
                    category="connectivity.aws",
                    check="aws_authentication",
                    severity=Severity.ERROR,
                    status="FAILED",
                    message=f"AWS authentication failed: {str(e)}",
                    suggestions=[
                        "Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
                        "Verify AWS credentials are not expired",
                        "Test with: aws sts get-caller-identity",
                    ],
                )
            )

        return results

    def test_database_connectivity(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Test database connectivity"""
        results = []

        rds_host = env_vars.get("RDS_HOST")
        if not rds_host:
            results.append(
                ValidationResult(
                    category="connectivity.database",
                    check="rds_configuration",
                    severity=Severity.INFO,
                    status="SKIPPED",
                    message="RDS_HOST not configured, skipping database connectivity test",
                )
            )
            return results

        if not HAS_CLOUD_LIBS:
            results.append(
                ValidationResult(
                    category="connectivity.database",
                    check="database_libraries",
                    severity=Severity.WARNING,
                    status="LIBRARIES_MISSING",
                    message="psycopg2 library not available for database connectivity testing",
                    suggestions=["Install psycopg2: pip install psycopg2-binary"],
                )
            )
            return results

        try:
            conn = psycopg2.connect(
                host=rds_host,
                port=env_vars.get("RDS_PORT", "5432"),
                database=env_vars.get("RDS_DB", "sales"),
                user=env_vars.get("RDS_USER"),
                password=env_vars.get("RDS_PASS"),
                connect_timeout=10,
            )
            conn.close()

            results.append(
                ValidationResult(
                    category="connectivity.database",
                    check="rds_connection",
                    severity=Severity.INFO,
                    status="SUCCESS",
                    message=f"Successfully connected to RDS database at {rds_host}",
                )
            )

        except Exception as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower():
                severity = Severity.ERROR
                suggestions = [
                    "Check if RDS_HOST is correct (get from terraform output)",
                    "Verify security group allows connections from ALLOWED_CIDR",
                    "Ensure RDS instance is running",
                ]
            elif (
                "authentication" in error_msg.lower() or "password" in error_msg.lower()
            ):
                severity = Severity.ERROR
                suggestions = [
                    "Check RDS_USER and RDS_PASS credentials",
                    "Verify database user exists and has proper permissions",
                ]
            else:
                severity = Severity.ERROR
                suggestions = [
                    "Check all RDS connection parameters",
                    "Verify network connectivity to RDS instance",
                ]

            results.append(
                ValidationResult(
                    category="connectivity.database",
                    check="rds_connection",
                    severity=severity,
                    status="FAILED",
                    message=f"Failed to connect to RDS database: {error_msg}",
                    suggestions=suggestions,
                )
            )

        return results

    def test_snowflake_connectivity(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Test Snowflake connectivity"""
        results = []

        snowflake_account = env_vars.get("SNOWFLAKE_ACCOUNT")
        if not snowflake_account or snowflake_account.startswith("YOUR_"):
            results.append(
                ValidationResult(
                    category="connectivity.snowflake",
                    check="snowflake_configuration",
                    severity=Severity.INFO,
                    status="SKIPPED",
                    message="Snowflake not configured, skipping connectivity test",
                )
            )
            return results

        if not HAS_CLOUD_LIBS:
            results.append(
                ValidationResult(
                    category="connectivity.snowflake",
                    check="snowflake_libraries",
                    severity=Severity.WARNING,
                    status="LIBRARIES_MISSING",
                    message="snowflake-connector-python not available for connectivity testing",
                    suggestions=[
                        "Install snowflake connector: pip install snowflake-connector-python"
                    ],
                )
            )
            return results

        try:
            conn = snowflake.connector.connect(
                account=snowflake_account,
                user=env_vars.get("SNOWFLAKE_USER"),
                password=env_vars.get("SNOWFLAKE_PASSWORD"),
                role=env_vars.get("SNOWFLAKE_ROLE", "SYSADMIN"),
                warehouse=env_vars.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                database=env_vars.get("SNOWFLAKE_DATABASE", "SALES_DW"),
                schema=env_vars.get("SNOWFLAKE_SCHEMA", "RAW"),
                login_timeout=10,
            )
            conn.close()

            results.append(
                ValidationResult(
                    category="connectivity.snowflake",
                    check="snowflake_connection",
                    severity=Severity.INFO,
                    status="SUCCESS",
                    message=f"Successfully connected to Snowflake account {snowflake_account}",
                )
            )

        except Exception as e:
            results.append(
                ValidationResult(
                    category="connectivity.snowflake",
                    check="snowflake_connection",
                    severity=Severity.ERROR,
                    status="FAILED",
                    message=f"Failed to connect to Snowflake: {str(e)}",
                    suggestions=[
                        "Check SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD",
                        "Verify Snowflake account identifier format (account.region.cloud)",
                        "Ensure user has access to specified warehouse and database",
                    ],
                )
            )

        return results

    def test_kafka_connectivity(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Test Kafka connectivity"""
        results = []

        bootstrap_servers = env_vars.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

        if not HAS_CLOUD_LIBS:
            results.append(
                ValidationResult(
                    category="connectivity.kafka",
                    check="kafka_libraries",
                    severity=Severity.WARNING,
                    status="LIBRARIES_MISSING",
                    message="kafka-python library not available for Kafka connectivity testing",
                    suggestions=["Install kafka-python: pip install kafka-python"],
                )
            )
            return results

        try:
            # Test producer connectivity
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                request_timeout_ms=5000,
                max_block_ms=5000,
            )
            producer.close()

            results.append(
                ValidationResult(
                    category="connectivity.kafka",
                    check="kafka_producer",
                    severity=Severity.INFO,
                    status="SUCCESS",
                    message=f"Successfully connected to Kafka at {bootstrap_servers}",
                )
            )

        except Exception as e:
            results.append(
                ValidationResult(
                    category="connectivity.kafka",
                    check="kafka_producer",
                    severity=Severity.ERROR,
                    status="FAILED",
                    message=f"Failed to connect to Kafka: {str(e)}",
                    suggestions=[
                        "Check KAFKA_BOOTSTRAP_SERVERS configuration",
                        "Ensure Kafka is running (docker-compose up kafka)",
                        "Verify network connectivity to Kafka brokers",
                    ],
                )
            )

        return results


class ConfigurationConsistencyChecker:
    """Checks consistency across configuration files"""

    def __init__(self, project_root: Path):
        self.project_root = project_root

    def check_cross_file_consistency(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check consistency between .env and other config files"""
        results = []

        # Check Terraform variables consistency
        results.extend(self._check_terraform_consistency(env_vars))

        # Check Docker Compose consistency
        results.extend(self._check_docker_compose_consistency(env_vars))

        # Check dbt project consistency
        results.extend(self._check_dbt_consistency(env_vars))

        # Check Airflow DAG consistency
        results.extend(self._check_airflow_dag_consistency(env_vars))

        return results

    def _check_terraform_consistency(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check consistency between .env and Terraform variables"""
        results = []

        terraform_vars_file = self.project_root / "infra" / "variables.tf"
        if not terraform_vars_file.exists():
            results.append(
                ValidationResult(
                    category="consistency.terraform",
                    check="variables_file_exists",
                    severity=Severity.WARNING,
                    status="FILE_MISSING",
                    message="Terraform variables file not found at infra/variables.tf",
                )
            )
            return results

        try:
            # Parse Terraform variables (basic parsing)
            with open(terraform_vars_file, "r") as f:
                tf_content = f.read()

            # Extract variable names
            tf_vars = re.findall(r'variable\s+"([^"]+)"', tf_content)

            # Check if .env variables map to TF_VAR_ equivalents
            env_to_tf_mapping = {
                "AWS_DEFAULT_REGION": "TF_VAR_AWS_REGION",
                "S3_BUCKET": "TF_VAR_RAW_BUCKET",
                "PROCESSED_BUCKET": "TF_VAR_PROCESSED_BUCKET",
                "RDS_DB": "TF_VAR_DB_NAME",
                "RDS_USER": "TF_VAR_DB_USERNAME",
                "RDS_PASS": "TF_VAR_DB_PASSWORD",
                "ALLOWED_CIDR": "TF_VAR_ALLOWED_CIDR",
                "PROJECT_NAME": "TF_VAR_PROJECT_NAME",
                "ENVIRONMENT": "TF_VAR_ENVIRONMENT",
            }

            for env_var, tf_var in env_to_tf_mapping.items():
                tf_var_name = tf_var.replace("TF_VAR_", "")
                if tf_var_name in tf_vars and not env_vars.get(env_var):
                    results.append(
                        ValidationResult(
                            category="consistency.terraform",
                            check=f"terraform_mapping_{env_var}",
                            severity=Severity.WARNING,
                            status="MISSING_ENV_VAR",
                            message=f"Terraform expects {tf_var} but {env_var} is not set in .env",
                            suggestions=[
                                f"Set {env_var} in your .env file",
                                f"This will be exported as {tf_var} for Terraform",
                            ],
                        )
                    )

            results.append(
                ValidationResult(
                    category="consistency.terraform",
                    check="terraform_variables_check",
                    severity=Severity.INFO,
                    status="CHECKED",
                    message="Terraform variables consistency check completed",
                    details={"terraform_variables_found": len(tf_vars)},
                )
            )

        except Exception as e:
            results.append(
                ValidationResult(
                    category="consistency.terraform",
                    check="terraform_parsing",
                    severity=Severity.WARNING,
                    status="PARSE_ERROR",
                    message=f"Could not parse Terraform variables: {str(e)}",
                )
            )

        return results

    def _check_docker_compose_consistency(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check Docker Compose configuration consistency"""
        results = []

        compose_files = [
            self.project_root / "docker-compose.yml",
            self.project_root / "docker-compose-cloud.yml",
        ]

        for compose_file in compose_files:
            if not compose_file.exists():
                continue

            try:
                with open(compose_file, "r") as f:
                    content = f.read()

                # Check for environment variable references
                env_refs = re.findall(r"\$\{([^}]+)\}", content)

                missing_vars = []
                for env_ref in env_refs:
                    # Handle default values like ${VAR:-default}
                    var_name = env_ref.split(":-")[0].split(":")[0]
                    if var_name not in env_vars and not var_name.startswith("_"):
                        missing_vars.append(var_name)

                if missing_vars:
                    results.append(
                        ValidationResult(
                            category="consistency.docker",
                            check=f"docker_compose_env_vars_{compose_file.name}",
                            severity=Severity.WARNING,
                            status="MISSING_VARS",
                            message=f"Docker Compose references undefined environment variables",
                            details={"missing_variables": missing_vars},
                            suggestions=[
                                "Add missing variables to your .env file",
                                "Check if variables have default values in docker-compose",
                            ],
                        )
                    )
                else:
                    results.append(
                        ValidationResult(
                            category="consistency.docker",
                            check=f"docker_compose_env_vars_{compose_file.name}",
                            severity=Severity.INFO,
                            status="CONSISTENT",
                            message=f"All Docker Compose environment variables are defined",
                        )
                    )

            except Exception as e:
                results.append(
                    ValidationResult(
                        category="consistency.docker",
                        check=f"docker_compose_parsing_{compose_file.name}",
                        severity=Severity.WARNING,
                        status="PARSE_ERROR",
                        message=f"Could not parse {compose_file.name}: {str(e)}",
                    )
                )

        return results

    def _check_dbt_consistency(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check dbt project configuration consistency"""
        results = []

        dbt_project_file = self.project_root / "dbt" / "dbt_project.yml"
        if not dbt_project_file.exists():
            results.append(
                ValidationResult(
                    category="consistency.dbt",
                    check="dbt_project_exists",
                    severity=Severity.INFO,
                    status="FILE_MISSING",
                    message="dbt project file not found, skipping dbt consistency check",
                )
            )
            return results

        try:
            with open(dbt_project_file, "r") as f:
                dbt_config = yaml.safe_load(f)

            # Check if dbt profile name matches
            profile_name = dbt_config.get("profile")
            if profile_name:
                results.append(
                    ValidationResult(
                        category="consistency.dbt",
                        check="dbt_profile_name",
                        severity=Severity.INFO,
                        status="CONFIGURED",
                        message=f"dbt project uses profile: {profile_name}",
                        suggestions=[
                            "Ensure your ~/.dbt/profiles.yml has a matching profile",
                            "Or configure profiles.yml in the dbt directory",
                        ],
                    )
                )

            # Check if Snowflake variables are set when dbt is configured
            snowflake_account = env_vars.get("SNOWFLAKE_ACCOUNT")
            if not snowflake_account or snowflake_account.startswith("YOUR_"):
                results.append(
                    ValidationResult(
                        category="consistency.dbt",
                        check="dbt_snowflake_config",
                        severity=Severity.WARNING,
                        status="INCOMPLETE_CONFIG",
                        message="dbt project found but Snowflake environment variables not configured",
                        suggestions=[
                            "Configure Snowflake variables in .env if using dbt with Snowflake",
                            "Or remove dbt directory if not using dbt transformations",
                        ],
                    )
                )

        except Exception as e:
            results.append(
                ValidationResult(
                    category="consistency.dbt",
                    check="dbt_project_parsing",
                    severity=Severity.WARNING,
                    status="PARSE_ERROR",
                    message=f"Could not parse dbt_project.yml: {str(e)}",
                )
            )

        return results

    def _check_airflow_dag_consistency(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check Airflow DAG configuration consistency"""
        results = []

        dag_files = list((self.project_root / "airflow" / "dags").glob("*_dag.py"))

        for dag_file in dag_files:
            try:
                with open(dag_file, "r") as f:
                    dag_content = f.read()

                # Extract environment variable references
                env_refs = re.findall(r'os\.getenv\(["\']([^"\']+)["\']', dag_content)

                missing_vars = []
                for var in env_refs:
                    if var not in env_vars and not var.startswith("_"):
                        missing_vars.append(var)

                if missing_vars:
                    results.append(
                        ValidationResult(
                            category="consistency.airflow",
                            check=f"airflow_dag_env_vars_{dag_file.name}",
                            severity=Severity.WARNING,
                            status="MISSING_VARS",
                            message=f"Airflow DAG {dag_file.name} references undefined environment variables",
                            details={"missing_variables": missing_vars},
                            suggestions=[
                                "Add missing variables to your .env file",
                                "Check if variables have default values in the DAG",
                            ],
                        )
                    )

            except Exception as e:
                results.append(
                    ValidationResult(
                        category="consistency.airflow",
                        check=f"airflow_dag_parsing_{dag_file.name}",
                        severity=Severity.WARNING,
                        status="PARSE_ERROR",
                        message=f"Could not parse {dag_file.name}: {str(e)}",
                    )
                )

        return results


class SecurityValidator:
    """Validates security best practices"""

    def validate_security_practices(
        self, env_vars: Dict[str, str], environment: Environment
    ) -> List[ValidationResult]:
        """Validate security best practices"""
        results = []

        # Check for secrets in environment variables
        results.extend(self._check_secrets_exposure(env_vars))

        # Validate network security
        results.extend(self._validate_network_security(env_vars, environment))

        # Check IAM and permissions
        results.extend(self._validate_iam_practices(env_vars))

        # Validate encryption settings
        results.extend(self._validate_encryption_settings(env_vars))

        return results

    def _check_secrets_exposure(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Check for potential secrets exposure"""
        results = []

        # Check for hardcoded secrets (patterns that might indicate exposed secrets)
        secret_patterns = {
            "aws_access_key": r"AKIA[0-9A-Z]{16}",
            "aws_secret_key": r"[A-Za-z0-9/+=]{40}",
            "databricks_token": r"dapi[a-f0-9]{32}",
            "private_key": r"-----BEGIN.*PRIVATE KEY-----",
        }

        for var_name, var_value in env_vars.items():
            for pattern_name, pattern in secret_patterns.items():
                if re.search(pattern, var_value):
                    results.append(
                        ValidationResult(
                            category="security.secrets",
                            check=f"hardcoded_secret_{var_name}",
                            severity=Severity.WARNING,
                            status="POTENTIAL_SECRET",
                            message=f"Variable {var_name} contains what appears to be a {pattern_name}",
                            suggestions=[
                                "Ensure .env file is in .gitignore",
                                "Never commit secrets to version control",
                                "Consider using AWS Secrets Manager or similar service",
                            ],
                        )
                    )

        return results

    def _validate_network_security(
        self, env_vars: Dict[str, str], environment: Environment
    ) -> List[ValidationResult]:
        """Validate network security configuration"""
        results = []

        cidr = env_vars.get("ALLOWED_CIDR", "")

        if cidr:
            try:
                network = ipaddress.IPv4Network(cidr, strict=False)

                # Check for overly broad access
                if network.num_addresses > 256:  # More than /24
                    severity = (
                        Severity.WARNING
                        if environment == Environment.DEV
                        else Severity.ERROR
                    )
                    results.append(
                        ValidationResult(
                            category="security.network",
                            check="cidr_too_broad",
                            severity=severity,
                            status="OVERLY_PERMISSIVE",
                            message=f"ALLOWED_CIDR {cidr} allows {network.num_addresses} IP addresses",
                            suggestions=[
                                "Restrict to smaller subnet for better security",
                                "Use /32 for single IP access",
                                "Consider VPN or bastion host for broader access",
                            ],
                        )
                    )

                # Check for public access in production
                if environment == Environment.PROD and network.is_global:
                    results.append(
                        ValidationResult(
                            category="security.network",
                            check="production_public_access",
                            severity=Severity.CRITICAL,
                            status="SECURITY_RISK",
                            message="Production environment allows public IP access",
                            suggestions=[
                                "Restrict production access to corporate network",
                                "Use VPN or private network for production access",
                                "Consider using AWS PrivateLink or VPC endpoints",
                            ],
                        )
                    )

            except ValueError as e:
                results.append(
                    ValidationResult(
                        category="security.network",
                        check="cidr_format",
                        severity=Severity.ERROR,
                        status="INVALID_CIDR",
                        message=f"Invalid CIDR format {cidr}: {str(e)}",
                        suggestions=["Use valid CIDR notation (e.g., 192.168.1.0/24)"],
                    )
                )

        # Check for insecure database access
        publicly_accessible = env_vars.get("PUBLICLY_ACCESSIBLE", "").lower()
        if publicly_accessible == "true" and environment == Environment.PROD:
            results.append(
                ValidationResult(
                    category="security.database",
                    check="public_database_access",
                    severity=Severity.ERROR,
                    status="SECURITY_RISK",
                    message="Database is configured as publicly accessible in production",
                    suggestions=[
                        "Set PUBLICLY_ACCESSIBLE=false for production",
                        "Use VPC and security groups for database access",
                        "Access database through bastion host or VPN",
                    ],
                )
            )

        return results

    def _validate_iam_practices(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Validate IAM and access practices"""
        results = []

        # Check if IAM role is configured (preferred over access keys)
        trusted_principal = env_vars.get("TRUSTED_PRINCIPAL_ARN")
        aws_access_key = env_vars.get("AWS_ACCESS_KEY_ID")

        if aws_access_key and not trusted_principal:
            results.append(
                ValidationResult(
                    category="security.iam",
                    check="iam_roles_vs_keys",
                    severity=Severity.WARNING,
                    status="IMPROVEMENT_AVAILABLE",
                    message="Using AWS access keys instead of IAM roles",
                    suggestions=[
                        "Configure TRUSTED_PRINCIPAL_ARN for IAM role-based access",
                        "IAM roles are more secure than long-term access keys",
                        "Consider using instance profiles or assume role patterns",
                    ],
                )
            )

        # Check for root access key usage (basic pattern check)
        if (
            aws_access_key
            and aws_access_key.startswith("AKIA")
            and "root" in aws_access_key.lower()
        ):
            results.append(
                ValidationResult(
                    category="security.iam",
                    check="root_access_keys",
                    severity=Severity.CRITICAL,
                    status="SECURITY_VIOLATION",
                    message="Possible root AWS access key detected",
                    suggestions=[
                        "Never use root access keys",
                        "Create IAM user with minimal necessary permissions",
                        "Enable MFA for AWS root account",
                    ],
                )
            )

        return results

    def _validate_encryption_settings(
        self, env_vars: Dict[str, str]
    ) -> List[ValidationResult]:
        """Validate encryption and secure communication settings"""
        results = []

        # Check Databricks host uses HTTPS
        databricks_host = env_vars.get("DATABRICKS_HOST", "")
        if databricks_host and not databricks_host.startswith("https://"):
            results.append(
                ValidationResult(
                    category="security.encryption",
                    check="databricks_https",
                    severity=Severity.ERROR,
                    status="INSECURE_CONNECTION",
                    message="Databricks host should use HTTPS",
                    suggestions=["Ensure DATABRICKS_HOST starts with https://"],
                )
            )

        # Check Snowflake account format (should imply secure connection)
        snowflake_account = env_vars.get("SNOWFLAKE_ACCOUNT", "")
        if snowflake_account and not any(
            cloud in snowflake_account for cloud in [".aws", ".azure", ".gcp"]
        ):
            results.append(
                ValidationResult(
                    category="security.encryption",
                    check="snowflake_account_format",
                    severity=Severity.WARNING,
                    status="VERIFY_FORMAT",
                    message="Snowflake account format may not specify cloud provider",
                    suggestions=[
                        "Use format: account.region.cloud (e.g., myaccount.us-east-1.aws)",
                        "This ensures secure connection to correct cloud provider",
                    ],
                )
            )

        return results


class SetupDoctor:
    """Main setup validation orchestrator"""

    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.env_validator = EnvironmentVariableValidator()
        self.connectivity_tester = ConnectivityTester()
        self.consistency_checker = ConfigurationConsistencyChecker(self.project_root)
        self.security_validator = SecurityValidator()

    def load_environment_variables(self, env_file: str = None) -> Dict[str, str]:
        """Load environment variables from .env file"""
        if env_file is None:
            env_file = self.project_root / ".env"
        else:
            env_file = Path(env_file)

        env_vars = {}

        if env_file.exists():
            with open(env_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        # Remove quotes
                        value = value.strip("\"'")
                        env_vars[key] = value

        # Also include current environment variables
        env_vars.update(os.environ)

        return env_vars

    def run_validation(
        self,
        environment: Environment,
        check_connectivity: bool = False,
        env_file: str = None,
    ) -> ValidationReport:
        """Run complete validation suite"""
        import datetime

        report = ValidationReport(
            environment=environment.value, timestamp=datetime.datetime.now().isoformat()
        )

        logger.info(f"Starting validation for {environment.value} environment...")

        # Load environment variables
        try:
            env_vars = self.load_environment_variables(env_file)
            logger.info(f"Loaded {len(env_vars)} environment variables")
        except Exception as e:
            report.add_result(
                ValidationResult(
                    category="environment.loading",
                    check="env_file_loading",
                    severity=Severity.CRITICAL,
                    status="FAILED",
                    message=f"Failed to load environment variables: {str(e)}",
                    suggestions=["Check if .env file exists and is readable"],
                )
            )
            return report

        # 1. Validate environment variables
        logger.info("Validating environment variables...")
        env_results = self.env_validator.validate_environment_variables(
            env_vars, environment
        )
        for result in env_results:
            report.add_result(result)

        # 2. Check cross-file consistency
        logger.info("Checking configuration consistency...")
        consistency_results = self.consistency_checker.check_cross_file_consistency(
            env_vars
        )
        for result in consistency_results:
            report.add_result(result)

        # 3. Validate security practices
        logger.info("Validating security practices...")
        security_results = self.security_validator.validate_security_practices(
            env_vars, environment
        )
        for result in security_results:
            report.add_result(result)

        # 4. Test connectivity (if requested)
        if check_connectivity:
            logger.info("Testing service connectivity...")

            # AWS connectivity
            aws_results = self.connectivity_tester.test_aws_connectivity(env_vars)
            for result in aws_results:
                report.add_result(result)

            # Database connectivity
            db_results = self.connectivity_tester.test_database_connectivity(env_vars)
            for result in db_results:
                report.add_result(result)

            # Snowflake connectivity
            snowflake_results = self.connectivity_tester.test_snowflake_connectivity(
                env_vars
            )
            for result in snowflake_results:
                report.add_result(result)

            # Kafka connectivity
            kafka_results = self.connectivity_tester.test_kafka_connectivity(env_vars)
            for result in kafka_results:
                report.add_result(result)

        logger.info("Validation completed")
        return report

    def print_report(self, report: ValidationReport, verbose: bool = False):
        """Print validation report to console"""
        print(f"\n{'='*80}")
        print(f"SALES DATA PIPELINE - SETUP VALIDATION REPORT")
        print(f"Environment: {report.environment.upper()}")
        print(f"Timestamp: {report.timestamp}")
        print(f"{'='*80}")

        # Summary
        total_checks = len(report.results)
        print(f"\nSUMMARY:")
        print(f"Total checks: {total_checks}")
        for severity, count in report.summary.items():
            icon = {"info": "✓", "warning": "⚠", "error": "✗", "critical": "!"}.get(
                severity, "?"
            )
            print(f"{icon} {severity.upper()}: {count}")

        # Group results by category
        categories = {}
        for result in report.results:
            category = result.category
            if category not in categories:
                categories[category] = []
            categories[category].append(result)

        # Print results by category
        for category, results in sorted(categories.items()):
            print(f"\n{category.upper()}:")
            print("-" * (len(category) + 1))

            for result in results:
                icon = {"INFO": "✓", "WARNING": "⚠", "ERROR": "✗", "CRITICAL": "!"}.get(
                    result.severity.value, "?"
                )
                status_color = {"VALID": "", "SUCCESS": "", "FAILED": "", "ERROR": ""}

                print(f"  {icon} [{result.status}] {result.message}")

                if verbose or result.severity in [
                    Severity.WARNING,
                    Severity.ERROR,
                    Severity.CRITICAL,
                ]:
                    if result.suggestions:
                        for suggestion in result.suggestions:
                            print(f"    → {suggestion}")

                    if result.details and verbose:
                        for key, value in result.details.items():
                            print(f"    📝 {key}: {value}")

        # Final recommendation
        print(f"\n{'='*80}")
        if report.has_errors():
            print("! SETUP ISSUES FOUND - Please address errors before proceeding")
            print("   Run with --verbose for detailed suggestions")
        elif report.has_warnings():
            print("⚠ SETUP MOSTLY READY - Some warnings to consider")
            print("   Review warnings for potential improvements")
        else:
            print("✓ SETUP VALIDATION PASSED - Ready for deployment!")
        print(f"{'='*80}\n")

    def save_report(self, report: ValidationReport, output_file: str):
        """Save validation report to JSON file"""
        report_dict = {
            "environment": report.environment,
            "timestamp": report.timestamp,
            "summary": report.summary,
            "results": [
                {
                    "category": r.category,
                    "check": r.check,
                    "severity": r.severity.value,
                    "status": r.status,
                    "message": r.message,
                    "suggestions": r.suggestions,
                    "details": r.details,
                }
                for r in report.results
            ],
        }

        with open(output_file, "w") as f:
            json.dump(report_dict, f, indent=2)

        logger.info(f"Report saved to {output_file}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Sales Data Pipeline Configuration Validator - Setup Doctor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python config_validator.py --env dev
  python config_validator.py --env prod --check-connectivity
  python config_validator.py --report-only --output report.json
  python config_validator.py --env-file /path/to/.env --verbose
        """,
    )

    parser.add_argument(
        "--env",
        type=str,
        choices=["dev", "staging", "prod", "test"],
        default="dev",
        help="Environment to validate (default: dev)",
    )

    parser.add_argument(
        "--check-connectivity",
        action="store_true",
        help="Test connectivity to external services (requires cloud libraries)",
    )

    parser.add_argument(
        "--env-file",
        type=str,
        help="Path to environment file (default: .env in current directory)",
    )

    parser.add_argument("--output", type=str, help="Save report to JSON file")

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output including all suggestions",
    )

    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Generate report without printing to console",
    )

    parser.add_argument(
        "--project-root",
        type=str,
        help="Project root directory (default: current directory)",
    )

    args = parser.parse_args()

    # Initialize setup doctor
    doctor = SetupDoctor(args.project_root)

    # Set up environment
    environment = Environment(args.env)

    # Run validation
    try:
        report = doctor.run_validation(
            environment=environment,
            check_connectivity=args.check_connectivity,
            env_file=args.env_file,
        )

        # Output results
        if not args.report_only:
            doctor.print_report(report, verbose=args.verbose)

        if args.output:
            doctor.save_report(report, args.output)

        # Exit with error code if there are critical issues
        if report.has_errors():
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Validation interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Validation failed with error: {str(e)}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
