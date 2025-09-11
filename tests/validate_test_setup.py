#!/usr/bin/env python3
"""
Test Setup Validation Script

This script validates that the test environment is properly configured
for running Kafka producer tests. It checks dependencies, imports,
and basic functionality.

Usage:
    python tests/validate_test_setup.py
"""

import sys
import os
import importlib
import logging
from typing import List, Tuple, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestSetupValidator:
    """Validates test environment setup"""

    def __init__(self):
        self.results = []
        self.error_count = 0
        self.warning_count = 0

    def log_result(
        self, test_name: str, success: bool, message: str, is_warning: bool = False
    ):
        """Log test result"""
        status = "✓" if success else ("⚠" if is_warning else "✗")
        level = (
            logging.WARNING
            if is_warning
            else (logging.INFO if success else logging.ERROR)
        )

        logger.log(level, f"{status} {test_name}: {message}")

        self.results.append(
            {
                "test": test_name,
                "success": success,
                "message": message,
                "warning": is_warning,
            }
        )

        if not success:
            if is_warning:
                self.warning_count += 1
            else:
                self.error_count += 1

    def check_python_version(self):
        """Check Python version compatibility"""
        version = sys.version_info
        if version.major == 3 and version.minor >= 8:
            self.log_result(
                "Python Version",
                True,
                f"Python {version.major}.{version.minor}.{version.micro} (compatible)",
            )
        else:
            self.log_result(
                "Python Version",
                False,
                f"Python {version.major}.{version.minor}.{version.micro} (requires 3.8+)",
            )

    def check_required_packages(self):
        """Check if required test packages are installed"""
        required_packages = [
            ("pytest", "Core testing framework"),
            ("pytest_cov", "Coverage reporting"),
            ("pytest_benchmark", "Performance testing"),
            ("kafka", "Kafka client library"),
            ("jsonschema", "Schema validation"),
            ("faker", "Test data generation"),
            ("psutil", "System monitoring"),
            ("memory_profiler", "Memory profiling"),
        ]

        for package_name, description in required_packages:
            try:
                importlib.import_module(package_name.replace("-", "_"))
                self.log_result(
                    f"Package: {package_name}", True, f"{description} - installed"
                )
            except ImportError:
                self.log_result(
                    f"Package: {package_name}",
                    False,
                    f"{description} - missing (install with: pip install {package_name})",
                )

    def check_test_files(self):
        """Check if test files exist and are accessible"""
        test_files = [
            ("test_kafka_producer.py", "Main test file"),
            ("run_kafka_producer_tests.py", "Test runner script"),
            ("pytest.ini", "Pytest configuration"),
            ("kafka_test_configs/docker_kafka_config.json", "Docker config"),
            ("kafka_test_configs/aws_msk_config.json", "AWS MSK config"),
        ]

        base_dir = os.path.dirname(os.path.abspath(__file__))

        for file_path, description in test_files:
            full_path = os.path.join(base_dir, file_path)
            if os.path.exists(full_path):
                self.log_result(f"File: {file_path}", True, f"{description} - found")
            else:
                self.log_result(f"File: {file_path}", False, f"{description} - missing")

    def check_kafka_producer_import(self):
        """Check if the Kafka producer module can be imported"""
        try:
            # Add streaming directory to path
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            streaming_dir = os.path.join(base_dir, "streaming")

            if streaming_dir not in sys.path:
                sys.path.insert(0, streaming_dir)

            from kafka_producer import SalesDataGenerator, KafkaSalesProducer

            self.log_result(
                "Kafka Producer Import",
                True,
                "Successfully imported SalesDataGenerator and KafkaSalesProducer",
            )

            # Test basic functionality
            generator = SalesDataGenerator()
            event = generator.generate_sales_event()

            self.log_result(
                "Data Generation Test",
                True,
                f"Generated sample event with order_id: {event.get('order_id', 'N/A')}",
            )

        except ImportError as e:
            self.log_result(
                "Kafka Producer Import",
                False,
                f"Failed to import kafka_producer module: {e}",
            )
        except Exception as e:
            self.log_result(
                "Data Generation Test", False, f"Failed to generate test data: {e}"
            )

    def check_kafka_connectivity(self):
        """Check Kafka connectivity (optional)"""
        try:
            from kafka import KafkaAdminClient

            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

            admin = KafkaAdminClient(
                bootstrap_servers=[bootstrap_servers], request_timeout_ms=5000
            )

            topics = admin.list_topics()
            admin.close()

            self.log_result(
                "Kafka Connectivity",
                True,
                f"Connected to Kafka at {bootstrap_servers}, found {len(topics)} topics",
                is_warning=False,
            )

        except Exception as e:
            self.log_result(
                "Kafka Connectivity",
                False,
                f"Cannot connect to Kafka: {e} (integration tests will be skipped)",
                is_warning=True,
            )

    def check_environment_variables(self):
        """Check important environment variables"""
        env_vars = [
            ("KAFKA_BOOTSTRAP_SERVERS", "Kafka connection string", "localhost:9092"),
            ("KAFKA_TOPIC", "Default Kafka topic", "sales_events"),
        ]

        for var_name, description, default_value in env_vars:
            value = os.getenv(var_name)
            if value:
                self.log_result(
                    f"Env Var: {var_name}", True, f"{description} = {value}"
                )
            else:
                self.log_result(
                    f"Env Var: {var_name}",
                    False,
                    f"{description} - not set (will use default: {default_value})",
                    is_warning=True,
                )

    def check_test_execution(self):
        """Test basic pytest execution"""
        try:
            import subprocess

            test_file = os.path.join(
                os.path.dirname(__file__), "test_kafka_producer.py"
            )

            # Run a simple syntax check
            result = subprocess.run(
                [sys.executable, "-m", "py_compile", test_file],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                self.log_result("Test File Syntax", True, "Test file syntax is valid")
            else:
                self.log_result(
                    "Test File Syntax",
                    False,
                    f"Syntax error in test file: {result.stderr}",
                )

        except Exception as e:
            self.log_result(
                "Test File Syntax", False, f"Failed to check test file syntax: {e}"
            )

    def run_all_checks(self):
        """Run all validation checks"""
        logger.info("Starting test setup validation...")
        logger.info("=" * 60)

        self.check_python_version()
        self.check_required_packages()
        self.check_test_files()
        self.check_kafka_producer_import()
        self.check_environment_variables()
        self.check_kafka_connectivity()
        self.check_test_execution()

        logger.info("=" * 60)

        # Summary
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r["success"])

        if self.error_count == 0:
            logger.info(f"Validation completed successfully!")
            logger.info(f"✓ {passed_checks}/{total_checks} checks passed")
            if self.warning_count > 0:
                logger.info(f"⚠ {self.warning_count} warnings (non-critical)")

            logger.info("\nReady to run tests:")
            logger.info("   python tests/run_kafka_producer_tests.py --mode quick")
            logger.info("   python tests/run_kafka_producer_tests.py --mode unit")

            return True
        else:
            logger.error(f"❌ Validation failed with {self.error_count} errors")
            logger.error("Please fix the errors above before running tests")

            if self.warning_count > 0:
                logger.warning(f"⚠ Also found {self.warning_count} warnings")

            return False

    def get_setup_instructions(self):
        """Get setup instructions based on validation results"""
        instructions = []

        for result in self.results:
            if not result["success"] and not result["warning"]:
                if "Package:" in result["test"]:
                    instructions.append(f"pip install {result['test'].split(': ')[1]}")
                elif "Kafka Producer Import" in result["test"]:
                    instructions.append(
                        "Ensure streaming/kafka_producer.py exists and is accessible"
                    )
                elif "File:" in result["test"]:
                    instructions.append(
                        f"Create missing file: {result['test'].split(': ')[1]}"
                    )

        if instructions:
            logger.info("\nSetup Instructions:")
            for i, instruction in enumerate(instructions, 1):
                logger.info(f"   {i}. {instruction}")

        return instructions


def main():
    """Main validation function"""
    validator = TestSetupValidator()
    success = validator.run_all_checks()

    if not success:
        validator.get_setup_instructions()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
