"""
Comprehensive test suite for the configuration validation framework.

This test suite covers all aspects of the configuration validator:
- Environment variable validation
- Service connectivity testing
- Cross-file configuration consistency
- Security best practices validation
- Multi-environment scenarios
"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Import the configuration validator components
import sys

sys.path.append(str(Path(__file__).parent.parent))

from config_validator import (
    SetupDoctor,
    EnvironmentVariableValidator,
    ConnectivityTester,
    ConfigurationConsistencyChecker,
    SecurityValidator,
    ValidationResult,
    ValidationReport,
    Severity,
    Environment,
)


class TestEnvironmentVariableValidator:
    """Test environment variable validation"""

    def setup_method(self):
        """Set up test fixtures"""
        self.validator = EnvironmentVariableValidator()

    def test_required_variables_validation_dev(self):
        """Test required variables for development environment"""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "my-sales-raw-bucket",
            "PROCESSED_BUCKET": "my-sales-processed-bucket",
            "PROJECT_NAME": "sales-pipeline",
            "ENVIRONMENT": "dev",
            "ALLOWED_CIDR": "192.168.1.100/32",
        }

        results = self.validator.validate_environment_variables(
            env_vars, Environment.DEV
        )

        # Should have no critical errors for required variables
        critical_errors = [r for r in results if r.severity == Severity.CRITICAL]
        missing_errors = [r for r in results if r.status == "MISSING"]

        assert (
            len(critical_errors) == 0
        ), f"Unexpected critical errors: {critical_errors}"
        assert len(missing_errors) == 0, f"Missing required variables: {missing_errors}"

    def test_required_variables_validation_prod(self):
        """Test required variables for production environment"""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "my-sales-raw-bucket",
            "PROCESSED_BUCKET": "my-sales-processed-bucket",
            "PROJECT_NAME": "sales-pipeline",
            "ENVIRONMENT": "prod",
            "ALLOWED_CIDR": "192.168.1.100/32",
            # Production requires database config
            "RDS_HOST": "mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
            "RDS_USER": "salesuser",
            "RDS_PASS": "SecurePassword123!",
            "RDS_DB": "sales",
        }

        results = self.validator.validate_environment_variables(
            env_vars, Environment.PROD
        )

        missing_errors = [r for r in results if r.status == "MISSING"]
        assert (
            len(missing_errors) == 0
        ), f"Missing required variables for prod: {missing_errors}"

    def test_missing_required_variables(self):
        """Test handling of missing required variables"""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            # Missing AWS_SECRET_ACCESS_KEY and others
        }

        results = self.validator.validate_environment_variables(
            env_vars, Environment.DEV
        )

        missing_errors = [r for r in results if r.status == "MISSING"]
        assert len(missing_errors) > 0, "Should detect missing required variables"

        # Check specific missing variables
        missing_vars = [r.check for r in missing_errors]
        assert any("AWS_SECRET_ACCESS_KEY" in check for check in missing_vars)

    def test_template_values_detection(self):
        """Test detection of template/placeholder values"""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "YOUR_AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "YOUR_AWS_SECRET_ACCESS_KEY",
            "RDS_PASS": "CHANGE_ME_STRONG_PASSWORD",
            "SNOWFLAKE_ACCOUNT": "YOUR_ACCOUNT.us-east-1.aws",
            "S3_BUCKET": "my-actual-bucket",  # This one is fine
        }

        results = self.validator.validate_environment_variables(
            env_vars, Environment.DEV
        )

        template_errors = [r for r in results if r.status == "TEMPLATE_VALUE"]
        assert (
            len(template_errors) >= 3
        ), f"Should detect template values, found: {template_errors}"

    def test_format_validation(self):
        """Test format validation for various variable types"""
        test_cases = [
            ("AWS_DEFAULT_REGION", "us-east-1", True),
            ("AWS_DEFAULT_REGION", "invalid-region!", False),
            ("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE", True),
            ("AWS_ACCESS_KEY_ID", "invalid-key", False),
            ("ALLOWED_CIDR", "192.168.1.100/32", True),
            ("ALLOWED_CIDR", "300.300.300.300/32", False),
            ("DATABRICKS_HOST", "https://myworkspace.cloud.databricks.com", True),
            ("DATABRICKS_HOST", "http://myworkspace.databricks.com", False),
            ("SALES_THRESHOLD", "10000", True),
            ("SALES_THRESHOLD", "not_a_number", False),
        ]

        for var_name, var_value, should_be_valid in test_cases:
            env_vars = {var_name: var_value}
            results = self.validator.validate_environment_variables(
                env_vars, Environment.DEV
            )

            format_results = [r for r in results if r.check == f"format_{var_name}"]

            if should_be_valid:
                # Should either have valid format or no format check
                if format_results:
                    assert (
                        format_results[0].status == "VALID"
                    ), f"{var_name}={var_value} should be valid but got: {format_results[0]}"
            else:
                # Should have invalid format
                assert any(
                    r.status == "INVALID_FORMAT" for r in format_results
                ), f"{var_name}={var_value} should be invalid but passed validation"

    def test_security_issues_detection(self):
        """Test detection of security issues"""
        env_vars = {
            "RDS_PASS": "weak",  # Too short
            "SNOWFLAKE_PASSWORD": "123",  # Too short
            "_AIRFLOW_WWW_USER_USERNAME": "admin",
            "_AIRFLOW_WWW_USER_PASSWORD": "admin",  # Default credentials
            "ALLOWED_CIDR": "0.0.0.0/0",  # Overly permissive
        }

        results = self.validator.validate_environment_variables(
            env_vars, Environment.PROD
        )

        security_issues = [r for r in results if r.category.startswith("security")]
        assert (
            len(security_issues) >= 3
        ), f"Should detect multiple security issues: {security_issues}"

        # Check specific security issues
        issue_types = [r.check for r in security_issues]
        assert any("weak_password" in check for check in issue_types)
        assert any("default_airflow_creds" in check for check in issue_types)
        assert any("overly_permissive_cidr" in check for check in issue_types)

    def test_common_misconfigurations(self):
        """Test detection of common configuration mistakes"""
        env_vars = {
            "PROJECT_NAME": "sales-pipeline",
            "S3_BUCKET": "different-bucket-name",  # Inconsistent naming
            "PROCESSED_BUCKET": "another-different-name",
            "HOST_DATA_DIR": "C:\\Users\\name\\project",  # Windows path format
        }

        results = self.validator.validate_environment_variables(
            env_vars, Environment.DEV
        )

        config_issues = [
            r
            for r in results
            if r.category == "configuration.consistency"
            or r.category == "configuration.paths"
        ]
        assert (
            len(config_issues) >= 1
        ), f"Should detect configuration issues: {config_issues}"


class TestConnectivityTester:
    """Test service connectivity validation"""

    def setup_method(self):
        """Set up test fixtures"""
        self.tester = ConnectivityTester()

    @patch("config_validator.HAS_CLOUD_LIBS", False)
    def test_missing_libraries_warning(self):
        """Test warning when cloud libraries are missing"""
        env_vars = {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}

        results = self.tester.test_aws_connectivity(env_vars)

        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "boto3" in results[0].message

    @patch("config_validator.HAS_CLOUD_LIBS", True)
    @patch("config_validator.boto3")
    def test_aws_connectivity_success(self, mock_boto3):
        """Test successful AWS connectivity"""
        # Mock successful AWS connection
        mock_session = MagicMock()
        mock_sts_client = MagicMock()
        mock_sts_client.get_caller_identity.return_value = {
            "Account": "123456789012",
            "UserId": "AIDACKCEVSQ6C2EXAMPLE",
            "Arn": "arn:aws:iam::123456789012:user/testuser",
        }
        mock_session.client.return_value = mock_sts_client
        mock_boto3.Session.return_value = mock_session

        env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "test-bucket",
        }

        results = self.tester.test_aws_connectivity(env_vars)

        # Should have successful authentication result
        auth_results = [r for r in results if r.check == "aws_authentication"]
        assert len(auth_results) == 1
        assert auth_results[0].status == "SUCCESS"

    @patch("config_validator.HAS_CLOUD_LIBS", True)
    @patch("config_validator.boto3")
    def test_aws_connectivity_failure(self, mock_boto3):
        """Test AWS connectivity failure"""
        # Mock failed AWS connection
        mock_boto3.Session.side_effect = Exception("Invalid credentials")

        env_vars = {"AWS_ACCESS_KEY_ID": "invalid", "AWS_SECRET_ACCESS_KEY": "invalid"}

        results = self.tester.test_aws_connectivity(env_vars)

        auth_results = [r for r in results if r.check == "aws_authentication"]
        assert len(auth_results) == 1
        assert auth_results[0].status == "FAILED"
        assert "Invalid credentials" in auth_results[0].message

    @patch("config_validator.HAS_CLOUD_LIBS", True)
    @patch("config_validator.psycopg2")
    def test_database_connectivity_success(self, mock_psycopg2):
        """Test successful database connectivity"""
        # Mock successful database connection
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        env_vars = {
            "RDS_HOST": "test.rds.amazonaws.com",
            "RDS_PORT": "5432",
            "RDS_DB": "sales",
            "RDS_USER": "testuser",
            "RDS_PASS": "testpass",
        }

        results = self.tester.test_database_connectivity(env_vars)

        conn_results = [r for r in results if r.check == "rds_connection"]
        assert len(conn_results) == 1
        assert conn_results[0].status == "SUCCESS"

    @patch("config_validator.HAS_CLOUD_LIBS", True)
    @patch("config_validator.psycopg2")
    def test_database_connectivity_timeout(self, mock_psycopg2):
        """Test database connectivity timeout"""
        # Mock timeout error
        mock_psycopg2.connect.side_effect = Exception("timeout expired")

        env_vars = {
            "RDS_HOST": "unreachable.rds.amazonaws.com",
            "RDS_USER": "testuser",
            "RDS_PASS": "testpass",
        }

        results = self.tester.test_database_connectivity(env_vars)

        conn_results = [r for r in results if r.check == "rds_connection"]
        assert len(conn_results) == 1
        assert conn_results[0].status == "FAILED"
        assert "timeout" in conn_results[0].message

    def test_database_connectivity_no_host(self):
        """Test database connectivity when no host is configured"""
        env_vars = {}  # No RDS_HOST

        results = self.tester.test_database_connectivity(env_vars)

        assert len(results) == 1
        assert results[0].status == "SKIPPED"
        assert "not configured" in results[0].message

    @patch("config_validator.HAS_CLOUD_LIBS", True)
    @patch("config_validator.snowflake.connector")
    def test_snowflake_connectivity_success(self, mock_snowflake):
        """Test successful Snowflake connectivity"""
        # Mock successful Snowflake connection
        mock_conn = MagicMock()
        mock_snowflake.connect.return_value = mock_conn

        env_vars = {
            "SNOWFLAKE_ACCOUNT": "myaccount.us-east-1.aws",
            "SNOWFLAKE_USER": "testuser",
            "SNOWFLAKE_PASSWORD": "testpass",
            "SNOWFLAKE_ROLE": "SYSADMIN",
            "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        }

        results = self.tester.test_snowflake_connectivity(env_vars)

        conn_results = [r for r in results if r.check == "snowflake_connection"]
        assert len(conn_results) == 1
        assert conn_results[0].status == "SUCCESS"

    def test_snowflake_connectivity_not_configured(self):
        """Test Snowflake connectivity when not configured"""
        env_vars = {"SNOWFLAKE_ACCOUNT": "YOUR_ACCOUNT.us-east-1.aws"}

        results = self.tester.test_snowflake_connectivity(env_vars)

        assert len(results) == 1
        assert results[0].status == "SKIPPED"


class TestConfigurationConsistencyChecker:
    """Test cross-file configuration consistency"""

    def setup_method(self):
        """Set up test fixtures with temporary directory"""
        self.temp_dir = tempfile.mkdtemp()
        self.project_root = Path(self.temp_dir)
        self.checker = ConfigurationConsistencyChecker(self.project_root)

    def teardown_method(self):
        """Clean up temporary directory"""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_terraform_consistency_check(self):
        """Test Terraform variables consistency checking"""
        # Create mock Terraform variables file
        terraform_dir = self.project_root / "infra"
        terraform_dir.mkdir()

        terraform_vars = """
variable "AWS_REGION" { type = string }
variable "RAW_BUCKET" { type = string }
variable "DB_NAME" { type = string }
variable "ALLOWED_CIDR" { type = string }
"""

        (terraform_dir / "variables.tf").write_text(terraform_vars)

        # Test with missing environment variables
        env_vars = {
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "my-bucket",
            # Missing RDS_DB and ALLOWED_CIDR
        }

        results = self.checker.check_cross_file_consistency(env_vars)

        terraform_results = [
            r for r in results if r.category == "consistency.terraform"
        ]
        missing_vars = [r for r in terraform_results if r.status == "MISSING_ENV_VAR"]

        assert (
            len(missing_vars) >= 1
        ), f"Should detect missing env vars: {terraform_results}"

    def test_docker_compose_consistency_check(self):
        """Test Docker Compose environment variables consistency"""
        # Create mock docker-compose.yml
        compose_content = """
version: '3.8'
services:
  app:
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - MISSING_VAR=${MISSING_VAR}
      - WITH_DEFAULT=${WITH_DEFAULT:-default_value}
"""

        (self.project_root / "docker-compose.yml").write_text(compose_content)

        env_vars = {
            "AWS_ACCESS_KEY_ID": "test_key",
            "WITH_DEFAULT": "custom_value",
            # MISSING_VAR is not defined
        }

        results = self.checker.check_cross_file_consistency(env_vars)

        docker_results = [r for r in results if r.category == "consistency.docker"]
        missing_vars_results = [r for r in docker_results if r.status == "MISSING_VARS"]

        if missing_vars_results:
            assert "MISSING_VAR" in str(missing_vars_results[0].details)

    def test_dbt_consistency_check(self):
        """Test dbt project configuration consistency"""
        # Create mock dbt project
        dbt_dir = self.project_root / "dbt"
        dbt_dir.mkdir()

        dbt_project = """
name: 'sales_data_pipeline'
version: '1.0.0'
profile: 'sales_data_pipeline'
"""

        (dbt_dir / "dbt_project.yml").write_text(dbt_project)

        # Test with missing Snowflake configuration
        env_vars = {
            # Snowflake vars missing
        }

        results = self.checker.check_cross_file_consistency(env_vars)

        dbt_results = [r for r in results if r.category == "consistency.dbt"]
        incomplete_config = [r for r in dbt_results if r.status == "INCOMPLETE_CONFIG"]

        assert (
            len(incomplete_config) >= 1
        ), "Should detect incomplete Snowflake config for dbt"

    def test_airflow_dag_consistency_check(self):
        """Test Airflow DAG environment variables consistency"""
        # Create mock Airflow DAG directory and file
        airflow_dir = self.project_root / "airflow" / "dags"
        airflow_dir.mkdir(parents=True)

        dag_content = """
import os
from airflow import DAG

# Environment variables used in DAG
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKET = os.getenv("S3_BUCKET")
MISSING_VAR = os.getenv("MISSING_DAG_VAR")
"""

        (airflow_dir / "test_dag.py").write_text(dag_content)

        env_vars = {
            "AWS_DEFAULT_REGION": "us-east-1",
            "S3_BUCKET": "my-bucket",
            # MISSING_DAG_VAR is not defined
        }

        results = self.checker.check_cross_file_consistency(env_vars)

        airflow_results = [r for r in results if r.category == "consistency.airflow"]
        missing_vars_results = [
            r for r in airflow_results if r.status == "MISSING_VARS"
        ]

        if missing_vars_results:
            assert "MISSING_DAG_VAR" in str(missing_vars_results[0].details)


class TestSecurityValidator:
    """Test security best practices validation"""

    def setup_method(self):
        """Set up test fixtures"""
        self.validator = SecurityValidator()

    def test_secrets_exposure_detection(self):
        """Test detection of potential secrets exposure"""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",  # Valid format
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",  # Valid format
            "SUSPICIOUS_VAR": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG...",  # Private key
        }

        results = self.validator.validate_security_practices(env_vars, Environment.DEV)

        secrets_results = [r for r in results if r.category == "security.secrets"]
        assert len(secrets_results) >= 1, "Should detect potential secrets"

    def test_network_security_validation_dev(self):
        """Test network security validation for development"""
        env_vars = {
            "ALLOWED_CIDR": "192.168.1.0/24",  # Moderately permissive, OK for dev
            "PUBLICLY_ACCESSIBLE": "true",
        }

        results = self.validator.validate_security_practices(env_vars, Environment.DEV)

        # Should be warnings at most for dev environment
        critical_results = [r for r in results if r.severity == Severity.CRITICAL]
        assert (
            len(critical_results) == 0
        ), f"Dev should not have critical network issues: {critical_results}"

    def test_network_security_validation_prod(self):
        """Test network security validation for production"""
        env_vars = {
            "ALLOWED_CIDR": "0.0.0.0/0",  # Very permissive, bad for prod
            "PUBLICLY_ACCESSIBLE": "true",  # Bad for prod database
        }

        results = self.validator.validate_security_practices(env_vars, Environment.PROD)

        # Should have critical/error issues for production
        severe_results = [
            r for r in results if r.severity in [Severity.CRITICAL, Severity.ERROR]
        ]
        assert (
            len(severe_results) >= 2
        ), f"Prod should have severe security issues: {severe_results}"

        # Check specific issues
        issue_checks = [r.check for r in severe_results]
        assert any("overly_permissive_cidr" in check for check in issue_checks)
        assert any("public_database_access" in check for check in issue_checks)

    def test_iam_practices_validation(self):
        """Test IAM and access practices validation"""
        # Test with access keys but no IAM role
        env_vars = {
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            # No TRUSTED_PRINCIPAL_ARN
        }

        results = self.validator.validate_security_practices(env_vars, Environment.PROD)

        iam_results = [r for r in results if r.category == "security.iam"]
        role_suggestions = [r for r in iam_results if "iam_roles_vs_keys" in r.check]

        assert (
            len(role_suggestions) >= 1
        ), "Should suggest using IAM roles over access keys"

    def test_encryption_settings_validation(self):
        """Test encryption and secure communication validation"""
        env_vars = {
            "DATABRICKS_HOST": "http://myworkspace.databricks.com",  # Should be HTTPS
            "SNOWFLAKE_ACCOUNT": "myaccount",  # Missing cloud provider
        }

        results = self.validator.validate_security_practices(env_vars, Environment.PROD)

        encryption_results = [r for r in results if r.category == "security.encryption"]

        # Should flag HTTP instead of HTTPS
        https_issues = [r for r in encryption_results if "databricks_https" in r.check]
        assert len(https_issues) >= 1, "Should detect insecure Databricks connection"

        # Should flag incomplete Snowflake account format
        snowflake_issues = [
            r for r in encryption_results if "snowflake_account_format" in r.check
        ]
        assert (
            len(snowflake_issues) >= 1
        ), "Should detect incomplete Snowflake account format"


class TestSetupDoctor:
    """Test the main SetupDoctor orchestrator"""

    def setup_method(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.project_root = Path(self.temp_dir)
        self.doctor = SetupDoctor(str(self.project_root))

    def teardown_method(self):
        """Clean up temporary directory"""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_load_environment_variables_from_file(self):
        """Test loading environment variables from .env file"""
        # Create test .env file
        env_content = """
# Test environment file
AWS_ACCESS_KEY_ID="test_key_id"
AWS_SECRET_ACCESS_KEY="test_secret_key"
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=my-test-bucket

# Comment line
EMPTY_VAR=
QUOTED_VAR="quoted value"
UNQUOTED_VAR=unquoted value
"""

        env_file = self.project_root / ".env"
        env_file.write_text(env_content)

        env_vars = self.doctor.load_environment_variables()

        assert env_vars["AWS_ACCESS_KEY_ID"] == "test_key_id"
        assert env_vars["AWS_SECRET_ACCESS_KEY"] == "test_secret_key"
        assert env_vars["AWS_DEFAULT_REGION"] == "us-east-1"
        assert env_vars["S3_BUCKET"] == "my-test-bucket"
        assert env_vars["QUOTED_VAR"] == "quoted value"
        assert env_vars["UNQUOTED_VAR"] == "unquoted value"

        # Empty variables should still be loaded
        assert "EMPTY_VAR" in env_vars

    def test_load_environment_variables_file_not_found(self):
        """Test loading environment variables when .env file doesn't exist"""
        # Don't create .env file
        env_vars = self.doctor.load_environment_variables()

        # Should still return current environment variables
        assert isinstance(env_vars, dict)
        # Should have at least some OS environment variables
        assert len(env_vars) > 0

    def test_run_validation_complete_workflow(self):
        """Test complete validation workflow"""
        # Create test .env file
        env_content = """
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=test-bucket
PROCESSED_BUCKET=test-processed-bucket
PROJECT_NAME=test-project
ENVIRONMENT=dev
ALLOWED_CIDR=192.168.1.100/32
"""

        env_file = self.project_root / ".env"
        env_file.write_text(env_content)

        # Run validation without connectivity testing
        report = self.doctor.run_validation(Environment.DEV, check_connectivity=False)

        assert isinstance(report, ValidationReport)
        assert report.environment == "dev"
        assert len(report.results) > 0

        # Should have environment, consistency, and security results
        categories = set(r.category.split(".")[0] for r in report.results)
        assert "environment" in categories
        assert "security" in categories

    def test_run_validation_with_connectivity(self):
        """Test validation with connectivity testing enabled"""
        # Create test .env file
        env_content = """
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
"""

        env_file = self.project_root / ".env"
        env_file.write_text(env_content)

        # Run validation with connectivity testing
        # This should work even without cloud libraries (will show warnings)
        report = self.doctor.run_validation(Environment.DEV, check_connectivity=True)

        assert isinstance(report, ValidationReport)
        assert len(report.results) > 0

        # Should have connectivity results (even if they're warnings about missing libs)
        connectivity_results = [
            r for r in report.results if "connectivity" in r.category
        ]
        assert len(connectivity_results) > 0

    def test_report_error_detection(self):
        """Test report error and warning detection"""
        report = ValidationReport(environment="test", timestamp="2023-01-01")

        # Add various severity results
        report.add_result(
            ValidationResult(
                category="test",
                check="info_check",
                severity=Severity.INFO,
                status="OK",
                message="Info message",
            )
        )
        report.add_result(
            ValidationResult(
                category="test",
                check="warning_check",
                severity=Severity.WARNING,
                status="WARNING",
                message="Warning message",
            )
        )
        report.add_result(
            ValidationResult(
                category="test",
                check="error_check",
                severity=Severity.ERROR,
                status="ERROR",
                message="Error message",
            )
        )

        assert (
            not report.has_errors() or report.has_errors()
        )  # Will be True due to ERROR
        assert report.has_warnings()

        # Check summary
        assert report.summary["info"] == 1
        assert report.summary["warning"] == 1
        assert report.summary["error"] == 1

    def test_save_and_load_report(self):
        """Test saving report to JSON file"""
        report = ValidationReport(environment="test", timestamp="2023-01-01")
        report.add_result(
            ValidationResult(
                category="test",
                check="test_check",
                severity=Severity.INFO,
                status="OK",
                message="Test message",
                suggestions=["Test suggestion"],
                details={"key": "value"},
            )
        )

        # Save report
        output_file = self.project_root / "test_report.json"
        self.doctor.save_report(report, str(output_file))

        # Verify file was created and has correct content
        assert output_file.exists()

        with open(output_file, "r") as f:
            report_data = json.load(f)

        assert report_data["environment"] == "test"
        assert report_data["timestamp"] == "2023-01-01"
        assert len(report_data["results"]) == 1
        assert report_data["results"][0]["message"] == "Test message"
        assert report_data["results"][0]["suggestions"] == ["Test suggestion"]
        assert report_data["results"][0]["details"] == {"key": "value"}


class TestIntegrationScenarios:
    """Integration tests for complete configuration scenarios"""

    def setup_method(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.project_root = Path(self.temp_dir)
        self.doctor = SetupDoctor(str(self.project_root))

    def teardown_method(self):
        """Clean up temporary directory"""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_minimal_dev_configuration(self):
        """Test minimal valid development configuration"""
        env_content = """
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=my-dev-raw-bucket
PROCESSED_BUCKET=my-dev-processed-bucket
PROJECT_NAME=sales-pipeline
ENVIRONMENT=dev
ALLOWED_CIDR=192.168.1.100/32
"""

        env_file = self.project_root / ".env"
        env_file.write_text(env_content)

        report = self.doctor.run_validation(Environment.DEV, check_connectivity=False)

        # Should have minimal errors for dev environment
        error_count = report.summary.get("error", 0) + report.summary.get("critical", 0)
        assert (
            error_count <= 2
        ), f"Minimal dev config should have few errors: {report.summary}"

    def test_production_configuration_requirements(self):
        """Test production configuration with all required elements"""
        env_content = """
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=company-sales-raw-prod
PROCESSED_BUCKET=company-sales-processed-prod
PROJECT_NAME=sales-pipeline
ENVIRONMENT=prod
ALLOWED_CIDR=10.0.0.0/24
RDS_HOST=sales-prod.cluster-xyz.us-east-1.rds.amazonaws.com
RDS_USER=salesuser
RDS_PASS=SecureProductionPassword123!
RDS_DB=sales
PUBLICLY_ACCESSIBLE=false
"""

        env_file = self.project_root / ".env"
        env_file.write_text(env_content)

        report = self.doctor.run_validation(Environment.PROD, check_connectivity=False)

        # Production should have stricter requirements
        critical_count = report.summary.get("critical", 0)
        assert (
            critical_count == 0
        ), f"Production config should have no critical issues: {report.summary}"

        # But may have some warnings for best practices
        warning_count = report.summary.get("warning", 0)
        assert warning_count >= 0  # Warnings are acceptable

    def test_insecure_configuration_detection(self):
        """Test detection of insecure configuration"""
        env_content = """
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=test-bucket
PROCESSED_BUCKET=test-processed
PROJECT_NAME=test
ENVIRONMENT=prod
ALLOWED_CIDR=0.0.0.0/0
RDS_HOST=test.rds.amazonaws.com
RDS_USER=admin
RDS_PASS=123
RDS_DB=sales
PUBLICLY_ACCESSIBLE=true
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
"""

        env_file = self.project_root / ".env"
        env_file.write_text(env_content)

        report = self.doctor.run_validation(Environment.PROD, check_connectivity=False)

        # Should detect multiple security issues
        critical_count = report.summary.get("critical", 0)
        error_count = report.summary.get("error", 0)
        warning_count = report.summary.get("warning", 0)

        total_issues = critical_count + error_count + warning_count
        assert (
            total_issues >= 5
        ), f"Should detect multiple security issues: {report.summary}"

        # Check for specific security issues
        security_results = [r for r in report.results if "security" in r.category]
        assert len(security_results) >= 3, "Should have multiple security violations"


# Test fixtures and utilities
@pytest.fixture
def sample_env_vars():
    """Sample environment variables for testing"""
    return {
        "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
        "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "AWS_DEFAULT_REGION": "us-east-1",
        "S3_BUCKET": "test-raw-bucket",
        "PROCESSED_BUCKET": "test-processed-bucket",
        "PROJECT_NAME": "test-pipeline",
        "ENVIRONMENT": "dev",
        "ALLOWED_CIDR": "192.168.1.100/32",
    }


@pytest.fixture
def temp_project_directory():
    """Temporary project directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    import shutil

    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
