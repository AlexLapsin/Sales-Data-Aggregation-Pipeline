#!/usr/bin/env python3
"""
Configuration Validator Demo Script

This script demonstrates the configuration validation framework capabilities
by running various test scenarios and showing the different types of validations.

Usage:
    python demo_config_validator.py --demo all
    python demo_config_validator.py --demo basic
    python demo_config_validator.py --demo security
    python demo_config_validator.py --demo connectivity
"""

import os
import sys
import tempfile
import argparse
from pathlib import Path
from typing import Dict

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config_validator import SetupDoctor, Environment


class ConfigValidatorDemo:
    """Demonstrates configuration validation capabilities"""

    def __init__(self):
        self.temp_dir = None
        self.doctor = None

    def setup_demo_environment(self) -> Path:
        """Set up a temporary directory for demo"""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="config_validator_demo_"))
        self.doctor = SetupDoctor(str(self.temp_dir))

        print(f"Demo environment created at: {self.temp_dir}")
        return self.temp_dir

    def cleanup_demo_environment(self):
        """Clean up demo environment"""
        if self.temp_dir and self.temp_dir.exists():
            import shutil

            shutil.rmtree(self.temp_dir)
            print(f"Demo environment cleaned up")

    def create_demo_config_file(self, scenario: str) -> Dict[str, str]:
        """Create a demo configuration file for testing"""
        configs = {
            "good": {
                "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
                "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "company-sales-raw-dev",
                "PROCESSED_BUCKET": "company-sales-processed-dev",
                "PROJECT_NAME": "sales-pipeline",
                "ENVIRONMENT": "dev",
                "ALLOWED_CIDR": "192.168.1.100/32",
                "RDS_USER": "salesuser",
                "RDS_PASS": "SecurePassword123!",
                "RDS_DB": "sales",
                "HOST_DATA_DIR": "/app/data",
                "PIPELINE_IMAGE": "sales-pipeline:v1.0.0",
            },
            "bad": {
                "AWS_ACCESS_KEY_ID": "YOUR_AWS_ACCESS_KEY_ID",  # Template value
                "AWS_SECRET_ACCESS_KEY": "YOUR_AWS_SECRET_ACCESS_KEY",  # Template value
                "AWS_DEFAULT_REGION": "invalid-region!",  # Invalid format
                "S3_BUCKET": "RAW_BUCKET_NAME",  # Template value
                "PROCESSED_BUCKET": "",  # Missing
                "PROJECT_NAME": "sales-pipeline",
                "ENVIRONMENT": "prod",
                "ALLOWED_CIDR": "0.0.0.0/0",  # Security risk
                "RDS_USER": "admin",
                "RDS_PASS": "123",  # Weak password
                "RDS_DB": "sales",
                "PUBLICLY_ACCESSIBLE": "true",  # Security risk in prod
                "_AIRFLOW_WWW_USER_USERNAME": "admin",
                "_AIRFLOW_WWW_USER_PASSWORD": "admin",  # Default credentials
                "HOST_DATA_DIR": "C:\\Users\\name\\project",  # Windows path
                "PIPELINE_IMAGE": "invalid image name!",  # Invalid format
            },
            "missing": {
                "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
                # Missing many required variables
                "S3_BUCKET": "test-bucket",
                "PROJECT_NAME": "test",
            },
            "security_issues": {
                "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
                "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "AWS_DEFAULT_REGION": "us-east-1",
                "S3_BUCKET": "test-bucket",
                "PROCESSED_BUCKET": "test-processed",
                "PROJECT_NAME": "test",
                "ENVIRONMENT": "prod",
                "ALLOWED_CIDR": "0.0.0.0/0",  # Critical security issue
                "RDS_PASS": "weak",  # Weak password
                "PUBLICLY_ACCESSIBLE": "true",  # Bad for prod
                "DATABRICKS_HOST": "http://insecure.databricks.com",  # HTTP instead of HTTPS
                "_AIRFLOW_WWW_USER_USERNAME": "admin",
                "_AIRFLOW_WWW_USER_PASSWORD": "admin",  # Default credentials
            },
        }

        return configs.get(scenario, configs["good"])

    def write_env_file(self, config: Dict[str, str]):
        """Write configuration to .env file"""
        env_file = self.temp_dir / ".env"

        with open(env_file, "w") as f:
            f.write("# Demo configuration file\n")
            for key, value in config.items():
                f.write(f'{key}="{value}"\n')

        return env_file

    def create_demo_project_files(self):
        """Create demo project files for consistency checking"""
        # Create Terraform variables file
        infra_dir = self.temp_dir / "infra"
        infra_dir.mkdir()

        terraform_vars = """
variable "AWS_REGION" { type = string }
variable "RAW_BUCKET" { type = string }
variable "PROCESSED_BUCKET" { type = string }
variable "DB_NAME" { type = string }
variable "ALLOWED_CIDR" { type = string }
variable "PROJECT_NAME" { type = string }
"""
        (infra_dir / "variables.tf").write_text(terraform_vars)

        # Create Docker Compose file
        docker_compose = """
version: '3.8'
services:
  app:
    image: ${PIPELINE_IMAGE}
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - UNKNOWN_VAR=${UNKNOWN_VAR}
      - WITH_DEFAULT=${WITH_DEFAULT:-default}
"""
        (self.temp_dir / "docker-compose.yml").write_text(docker_compose)

        # Create dbt project
        dbt_dir = self.temp_dir / "dbt"
        dbt_dir.mkdir()

        dbt_project = """
name: 'sales_data_pipeline'
version: '1.0.0'
profile: 'sales_data_pipeline'
"""
        (dbt_dir / "dbt_project.yml").write_text(dbt_project)

        # Create Airflow DAG
        airflow_dir = self.temp_dir / "airflow" / "dags"
        airflow_dir.mkdir(parents=True)

        dag_content = """
import os
from airflow import DAG

AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKET = os.getenv("S3_BUCKET")
MISSING_VAR = os.getenv("UNDEFINED_VAR")
"""
        (airflow_dir / "test_dag.py").write_text(dag_content)

    def demo_basic_validation(self):
        """Demonstrate basic configuration validation"""
        print("\n" + "=" * 60)
        print("DEMO: Basic Configuration Validation")
        print("=" * 60)

        # Test good configuration
        print("\nTesting GOOD configuration...")
        good_config = self.create_demo_config_file("good")
        self.write_env_file(good_config)

        report = self.doctor.run_validation(Environment.DEV, check_connectivity=False)

        print(f"Results: {report.summary}")
        if report.has_errors():
            print("❌ Unexpected errors in good configuration!")
        else:
            print("✓ Good configuration validated successfully")

        # Test bad configuration
        print("\nTesting BAD configuration...")
        bad_config = self.create_demo_config_file("bad")
        self.write_env_file(bad_config)

        report = self.doctor.run_validation(Environment.PROD, check_connectivity=False)

        print(f"Results: {report.summary}")
        if report.has_errors():
            print("✓ Successfully detected configuration errors")

            # Show some example errors
            error_results = [
                r for r in report.results if r.severity.value in ["ERROR", "CRITICAL"]
            ]
            print(f"\nExample errors detected ({len(error_results)} total):")
            for i, result in enumerate(error_results[:3]):
                print(f"  {i+1}. {result.message}")
        else:
            print("❌ Failed to detect configuration errors!")

    def demo_security_validation(self):
        """Demonstrate security validation"""
        print("\n" + "=" * 60)
        print("🔒 DEMO: Security Validation")
        print("=" * 60)

        security_config = self.create_demo_config_file("security_issues")
        self.write_env_file(security_config)

        report = self.doctor.run_validation(Environment.PROD, check_connectivity=False)

        # Focus on security results
        security_results = [r for r in report.results if "security" in r.category]

        print(f"Security issues found: {len(security_results)}")

        for category in [
            "security.network",
            "security.credentials",
            "security.encryption",
        ]:
            category_results = [r for r in security_results if r.category == category]
            if category_results:
                print(f"\n{category.upper().replace('.', ' ')} Issues:")
                for result in category_results:
                    severity_icon = {"CRITICAL": "!", "ERROR": "X", "WARNING": "⚠"}.get(
                        result.severity.value, "i"
                    )
                    print(f"  {severity_icon} {result.message}")
                    if result.suggestions:
                        for suggestion in result.suggestions[
                            :2
                        ]:  # Show first 2 suggestions
                            print(f"    → {suggestion}")

    def demo_consistency_checking(self):
        """Demonstrate cross-file consistency checking"""
        print("\n" + "=" * 60)
        print("🔗 DEMO: Cross-File Consistency Checking")
        print("=" * 60)

        # Create project files
        self.create_demo_project_files()

        # Create config with missing variables
        config = self.create_demo_config_file("missing")
        self.write_env_file(config)

        report = self.doctor.run_validation(Environment.DEV, check_connectivity=False)

        # Focus on consistency results
        consistency_results = [r for r in report.results if "consistency" in r.category]

        print(f"Consistency issues found: {len(consistency_results)}")

        for result in consistency_results:
            print(f"\n📁 {result.category}: {result.message}")
            if hasattr(result, "details") and result.details:
                for key, value in result.details.items():
                    print(f"   📝 {key}: {value}")

    def demo_environment_scenarios(self):
        """Demonstrate different environment validation scenarios"""
        print("\n" + "=" * 60)
        print("🌍 DEMO: Environment-Specific Validation")
        print("=" * 60)

        config = self.create_demo_config_file("security_issues")
        self.write_env_file(config)

        environments = [Environment.DEV, Environment.STAGING, Environment.PROD]

        for env in environments:
            print(f"\nValidating for {env.value.upper()} environment:")

            report = self.doctor.run_validation(env, check_connectivity=False)

            critical_count = report.summary.get("critical", 0)
            error_count = report.summary.get("error", 0)
            warning_count = report.summary.get("warning", 0)

            print(
                f"  Critical: {critical_count}, Errors: {error_count}, Warnings: {warning_count}"
            )

            # Production should have more strict validation
            if env == Environment.PROD:
                if critical_count > 0:
                    print("  ✓ Correctly flagged critical issues for production")
                else:
                    print("  ⚠ Should have flagged critical issues for production")

    def demo_report_generation(self):
        """Demonstrate report generation and saving"""
        print("\n" + "=" * 60)
        print("DEMO: Report Generation")
        print("=" * 60)

        config = self.create_demo_config_file("bad")
        self.write_env_file(config)

        report = self.doctor.run_validation(Environment.PROD, check_connectivity=False)

        # Save report to JSON
        report_file = self.temp_dir / "validation_report.json"
        self.doctor.save_report(report, str(report_file))

        print(f"✓ Report saved to: {report_file}")
        print(f"📏 Report size: {report_file.stat().st_size} bytes")

        # Show report structure
        import json

        with open(report_file, "r") as f:
            report_data = json.load(f)

        print(f"Report contains:")
        print(f"   Environment: {report_data['environment']}")
        print(f"   Timestamp: {report_data['timestamp']}")
        print(f"   Results: {len(report_data['results'])} checks")
        print(f"   Summary: {report_data['summary']}")

    def run_all_demos(self):
        """Run all demo scenarios"""
        try:
            self.setup_demo_environment()

            print("CONFIGURATION VALIDATOR DEMONSTRATION")
            print(
                "This demo shows the capabilities of the configuration validation framework"
            )

            self.demo_basic_validation()
            self.demo_security_validation()
            self.demo_consistency_checking()
            self.demo_environment_scenarios()
            self.demo_report_generation()

            print("\n" + "=" * 60)
            print("✓ DEMO COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print("\nNext steps to use the configuration validator:")
            print(
                "1. Install dependencies: pip install -r requirements-config-validator.txt"
            )
            print("2. Generate a template: python config_templates.py --template dev")
            print("3. Create your .env: cp .env.dev .env")
            print("4. Edit .env with your values")
            print("5. Run validation: python setup_doctor.py")
            print(
                "6. Run full validation: python config_validator.py --env prod --check-connectivity"
            )

        except Exception as e:
            print(f"❌ Demo failed with error: {e}")
            import traceback

            traceback.print_exc()
        finally:
            self.cleanup_demo_environment()


def run_test_suite():
    """Run the test suite to demonstrate testing capabilities"""
    print("\n" + "=" * 60)
    print("RUNNING TEST SUITE")
    print("=" * 60)

    try:
        import pytest

        # Run the test suite
        test_file = Path(__file__).parent / "tests" / "test_config_validator.py"

        if test_file.exists():
            print(f"Running tests from: {test_file}")

            # Run pytest with verbose output
            result = pytest.main(
                [str(test_file), "-v", "--tb=short", "--disable-warnings"]
            )

            if result == 0:
                print("✓ All tests passed!")
            else:
                print(f"❌ Some tests failed (exit code: {result})")

            return result == 0
        else:
            print(f"❌ Test file not found: {test_file}")
            return False

    except ImportError:
        print("❌ pytest not installed. Install with: pip install pytest")
        return False
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        return False


def main():
    """Main demo entry point"""
    parser = argparse.ArgumentParser(
        description="Configuration Validator Demo - Shows framework capabilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Demo Options:
  basic         - Basic configuration validation
  security      - Security validation features
  consistency   - Cross-file consistency checking
  environments  - Environment-specific validation
  reports       - Report generation and saving
  all           - Run all demos
  tests         - Run test suite
        """,
    )

    parser.add_argument(
        "--demo",
        choices=[
            "basic",
            "security",
            "consistency",
            "environments",
            "reports",
            "all",
            "tests",
        ],
        default="all",
        help="Which demo to run (default: all)",
    )

    args = parser.parse_args()

    demo = ConfigValidatorDemo()

    if args.demo == "tests":
        success = run_test_suite()
    elif args.demo == "all":
        demo.run_all_demos()
        success = True
    else:
        # Run specific demo
        try:
            demo.setup_demo_environment()

            if args.demo == "basic":
                demo.demo_basic_validation()
            elif args.demo == "security":
                demo.demo_security_validation()
            elif args.demo == "consistency":
                demo.demo_consistency_checking()
            elif args.demo == "environments":
                demo.demo_environment_scenarios()
            elif args.demo == "reports":
                demo.demo_report_generation()

            success = True

        except Exception as e:
            print(f"❌ Demo failed: {e}")
            success = False
        finally:
            demo.cleanup_demo_environment()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
