#!/usr/bin/env python3
"""
Comprehensive test runner for Spark ETL job tests.

This script provides different test execution modes and configurations
for various testing scenarios and environments.
"""

import os
import sys
import argparse
import subprocess
from typing import List, Dict, Optional
import time


class TestRunner:
    """Test runner for Spark ETL tests with various execution modes."""

    def __init__(self):
        self.spark_available = self._check_spark_availability()
        self.snowflake_available = self._check_snowflake_availability()
        self.aws_available = self._check_aws_availability()

    def _check_spark_availability(self) -> bool:
        """Check if PySpark is available."""
        try:
            import pyspark

            return True
        except ImportError:
            return False

    def _check_snowflake_availability(self) -> bool:
        """Check if Snowflake connector is available."""
        try:
            import snowflake.connector

            return True
        except ImportError:
            return False

    def _check_aws_availability(self) -> bool:
        """Check if AWS SDK is available."""
        try:
            import boto3

            return True
        except ImportError:
            return False

    def _check_snowflake_credentials(self) -> bool:
        """Check if Snowflake credentials are configured."""
        required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
        return all(os.getenv(var) for var in required_vars)

    def _check_aws_credentials(self) -> bool:
        """Check if AWS credentials are configured."""
        return bool(
            os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")
        )

    def run_command(self, cmd: List[str], description: str) -> bool:
        """Run a command and return success status."""
        print(f"\n🔄 {description}")
        print(f"Command: {' '.join(cmd)}")

        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = time.time() - start_time

        if result.returncode == 0:
            print(f"{description} completed successfully in {duration:.2f}s")
            if result.stdout:
                print("Output:", result.stdout[-500:])  # Last 500 chars
            return True
        else:
            print(f"❌ {description} failed after {duration:.2f}s")
            if result.stderr:
                print("Error:", result.stderr)
            if result.stdout:
                print("Output:", result.stdout)
            return False

    def run_unit_tests(self) -> bool:
        """Run unit tests (fast, no external dependencies)."""
        cmd = [
            "python",
            "-m",
            "pytest",
            "-m",
            "not integration and not slow and not performance",
            "--tb=short",
            "-v",
        ]

        if not self.spark_available:
            cmd.extend(["-k", "not spark"])

        return self.run_command(cmd, "Running unit tests")

    def run_integration_tests(self) -> bool:
        """Run integration tests (require external services)."""
        if not self.spark_available:
            print("WARNING: Skipping integration tests - PySpark not available")
            return True

        # Check for required credentials
        missing_deps = []
        if not self._check_snowflake_credentials():
            missing_deps.append("Snowflake credentials")
        if not self._check_aws_credentials():
            missing_deps.append("AWS credentials")

        if missing_deps:
            print(
                f"WARNING: Integration tests may be limited - missing: {', '.join(missing_deps)}"
            )

        cmd = ["python", "-m", "pytest", "-m", "integration", "--tb=short", "-v"]

        return self.run_command(cmd, "Running integration tests")

    def run_performance_tests(self) -> bool:
        """Run performance and benchmark tests."""
        if not self.spark_available:
            print("WARNING: Skipping performance tests - PySpark not available")
            return True

        cmd = [
            "python",
            "-m",
            "pytest",
            "-m",
            "performance or benchmark",
            "--tb=short",
            "-v",
        ]

        return self.run_command(cmd, "Running performance tests")

    def run_all_tests(self) -> bool:
        """Run all tests."""
        cmd = ["python", "-m", "pytest", "--tb=short", "-v"]

        if not self.spark_available:
            cmd.extend(["-k", "not spark"])

        return self.run_command(cmd, "Running all tests")

    def run_coverage_tests(self) -> bool:
        """Run tests with coverage reporting."""
        cmd = [
            "python",
            "-m",
            "pytest",
            "--cov=.",
            "--cov-report=html",
            "--cov-report=term-missing",
            "--tb=short",
            "-v",
        ]

        if not self.spark_available:
            cmd.extend(["-k", "not spark"])

        success = self.run_command(cmd, "Running tests with coverage")

        if success:
            print("\nCoverage report generated in htmlcov/index.html")

        return success

    def run_specific_test(self, test_pattern: str) -> bool:
        """Run specific test(s) matching the pattern."""
        cmd = ["python", "-m", "pytest", "-k", test_pattern, "--tb=short", "-v"]

        return self.run_command(cmd, f"Running tests matching: {test_pattern}")

    def run_test_file(self, test_file: str) -> bool:
        """Run tests from a specific file."""
        if not os.path.exists(test_file):
            print(f"❌ Test file not found: {test_file}")
            return False

        cmd = ["python", "-m", "pytest", test_file, "--tb=short", "-v"]

        return self.run_command(cmd, f"Running tests from: {test_file}")

    def validate_environment(self) -> Dict[str, bool]:
        """Validate test environment and dependencies."""
        validation_results = {
            "PySpark": self.spark_available,
            "Snowflake Connector": self.snowflake_available,
            "AWS SDK": self.aws_available,
            "Snowflake Credentials": self._check_snowflake_credentials(),
            "AWS Credentials": self._check_aws_credentials(),
        }

        print("\nEnvironment Validation:")
        print("=" * 40)

        for component, available in validation_results.items():
            status = "AVAILABLE" if available else "MISSING"
            print(f"{status} {component}")

        # Check for pytest and other test dependencies
        try:
            import pytest

            print("AVAILABLE: pytest")
        except ImportError:
            print("❌ pytest")
            validation_results["pytest"] = False

        try:
            import faker

            print("AVAILABLE: faker")
        except ImportError:
            print("❌ faker")
            validation_results["faker"] = False

        try:
            import psutil

            print("AVAILABLE: psutil")
        except ImportError:
            print("❌ psutil")
            validation_results["psutil"] = False

        return validation_results

    def print_test_summary(self):
        """Print summary of available tests."""
        print("\nAvailable Test Suites:")
        print("=" * 50)

        test_files = [
            ("conftest.py", "Test fixtures and utilities"),
            ("test_sales_etl_job.py", "Unit tests for SalesETLJob class"),
            ("test_data_quality.py", "Data quality and validation tests"),
            ("test_integration.py", "Integration tests with external services"),
            ("test_performance.py", "Performance and stress tests"),
            ("test_error_handling.py", "Error handling and edge cases"),
            ("test_config.py", "Configuration and environment tests"),
        ]

        for filename, description in test_files:
            exists = "EXISTS" if os.path.exists(filename) else "MISSING"
            print(f"{exists} {filename:25} - {description}")

        print("\nTest Markers:")
        markers = [
            ("unit", "Fast unit tests with no external dependencies"),
            ("integration", "Integration tests requiring external services"),
            ("performance", "Performance and benchmark tests"),
            ("slow", "Slow-running tests"),
            ("spark", "Tests requiring PySpark"),
            ("snowflake", "Tests requiring Snowflake connectivity"),
            ("aws", "Tests requiring AWS services"),
        ]

        for marker, description in markers:
            print(f"   {marker:12} - {description}")


def main():
    """Main entry point for test runner."""
    parser = argparse.ArgumentParser(description="Spark ETL Test Runner")
    parser.add_argument(
        "mode",
        choices=[
            "unit",
            "integration",
            "performance",
            "all",
            "coverage",
            "validate",
            "summary",
        ],
        nargs="?",
        default="unit",
        help="Test execution mode",
    )
    parser.add_argument("-k", "--pattern", help="Run tests matching the given pattern")
    parser.add_argument("-f", "--file", help="Run tests from specific file")
    parser.add_argument(
        "--list-tests",
        action="store_true",
        help="List available tests without running them",
    )

    args = parser.parse_args()

    runner = TestRunner()

    print("Spark ETL Test Runner")
    print("=" * 50)

    if args.list_tests:
        cmd = ["python", "-m", "pytest", "--collect-only", "-q"]
        runner.run_command(cmd, "Listing available tests")
        return

    # Handle specific patterns or files
    if args.pattern:
        success = runner.run_specific_test(args.pattern)
    elif args.file:
        success = runner.run_test_file(args.file)
    elif args.mode == "unit":
        success = runner.run_unit_tests()
    elif args.mode == "integration":
        success = runner.run_integration_tests()
    elif args.mode == "performance":
        success = runner.run_performance_tests()
    elif args.mode == "all":
        success = runner.run_all_tests()
    elif args.mode == "coverage":
        success = runner.run_coverage_tests()
    elif args.mode == "validate":
        validation_results = runner.validate_environment()
        success = all(validation_results.values())
    elif args.mode == "summary":
        runner.print_test_summary()
        return
    else:
        print(f"❌ Unknown mode: {args.mode}")
        return

    # Print final result
    if success:
        print("\nAll tests completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
