#!/usr/bin/env python3
"""
Kafka Producer Test Runner

This script provides different modes for running Kafka producer tests:
1. Unit tests only (no Kafka required)
2. Integration tests (requires Kafka)
3. Performance tests
4. Full test suite
5. Schema validation tests

Usage:
    python run_kafka_producer_tests.py --mode unit
    python run_kafka_producer_tests.py --mode integration
    python run_kafka_producer_tests.py --mode performance
    python run_kafka_producer_tests.py --mode all
    python run_kafka_producer_tests.py --mode schema

Requirements:
    - Install test dependencies: pip install -r requirements/test.txt
    - For integration tests: Ensure Kafka is running
    - For performance tests: Ensure adequate system resources
"""

import argparse
import sys
import os
import subprocess
import logging
import time
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaProducerTestRunner:
    """Test runner for Kafka Producer tests"""

    def __init__(self):
        self.test_file = "test_kafka_producer.py"
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

    def check_dependencies(self) -> bool:
        """Check if required test dependencies are installed"""
        required_packages = [
            "pytest",
            "pytest-cov",
            "pytest-benchmark",
            "kafka-python",
            "jsonschema",
            "faker",
            "psutil",
            "memory-profiler",
        ]

        missing_packages = []
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
            except ImportError:
                missing_packages.append(package)

        if missing_packages:
            logger.error(f"Missing required packages: {missing_packages}")
            logger.error("Install with: pip install -r requirements/test.txt")
            return False

        return True

    def check_kafka_availability(self) -> bool:
        """Check if Kafka is available for integration tests"""
        try:
            from kafka import KafkaAdminClient

            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

            admin = KafkaAdminClient(
                bootstrap_servers=[bootstrap_servers], request_timeout_ms=5000
            )
            admin.list_topics()
            admin.close()
            logger.info("Kafka is available for integration tests")
            return True
        except Exception as e:
            logger.warning(f"Kafka not available: {e}")
            return False

    def run_tests(
        self, test_markers: List[str] = None, extra_args: List[str] = None
    ) -> bool:
        """Run tests with specified markers and arguments"""
        if not self.check_dependencies():
            return False

        cmd = ["python", "-m", "pytest", self.test_file]

        # Add markers if specified
        if test_markers:
            marker_expr = " or ".join(test_markers)
            cmd.extend(["-m", marker_expr])

        # Add extra arguments
        if extra_args:
            cmd.extend(extra_args)

        logger.info(f"Running command: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, cwd=self.base_dir, capture_output=False)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Test execution failed: {e}")
            return False

    def run_unit_tests(self) -> bool:
        """Run unit tests only (no external dependencies)"""
        logger.info("Running unit tests (no Kafka required)...")
        return self.run_tests(
            test_markers=["unit"],
            extra_args=[
                "--cov=src.streaming.producers",
                "--cov-report=term-missing",
                "--cov-report=html:htmlcov/unit",
                "-v",
            ],
        )

    def run_integration_tests(self) -> bool:
        """Run integration tests (requires Kafka)"""
        logger.info("Running integration tests (Kafka required)...")

        if not self.check_kafka_availability():
            logger.error("Kafka not available - skipping integration tests")
            return False

        return self.run_tests(
            test_markers=["integration"], extra_args=["-v", "--tb=short"]
        )

    def run_performance_tests(self) -> bool:
        """Run performance tests"""
        logger.info("Running performance tests...")

        return self.run_tests(
            test_markers=["performance"],
            extra_args=[
                "--benchmark-only",
                "--benchmark-sort=mean",
                "--benchmark-json=benchmark_results.json",
                "-v",
            ],
        )

    def run_schema_tests(self) -> bool:
        """Run schema validation tests"""
        logger.info("Running schema validation tests...")

        return self.run_tests(test_markers=["schema"], extra_args=["-v"])

    def run_error_handling_tests(self) -> bool:
        """Run error handling tests"""
        logger.info("Running error handling tests...")

        return self.run_tests(test_markers=["error_handling"], extra_args=["-v"])

    def run_config_tests(self) -> bool:
        """Run configuration tests"""
        logger.info("Running configuration tests...")

        return self.run_tests(test_markers=["config"], extra_args=["-v"])

    def run_all_tests(self) -> bool:
        """Run complete test suite"""
        logger.info("Running complete test suite...")

        kafka_available = self.check_kafka_availability()

        extra_args = [
            "--cov=src.streaming.producers",
            "--cov-report=term-missing",
            "--cov-report=html:htmlcov/all",
            "--junit-xml=test_results.xml",
            "-v",
        ]

        if not kafka_available:
            logger.warning("Kafka not available - skipping integration tests")
            extra_args.extend(["-m", "not integration"])

        return self.run_tests(extra_args=extra_args)

    def run_quick_tests(self) -> bool:
        """Run quick tests for development (unit + schema)"""
        logger.info("Running quick tests for development...")

        return self.run_tests(
            test_markers=["unit", "schema"], extra_args=["-v", "--tb=line"]
        )

    def generate_test_report(self) -> None:
        """Generate comprehensive test report"""
        logger.info("Generating comprehensive test report...")

        # Run all tests with detailed reporting
        cmd = [
            "python",
            "-m",
            "pytest",
            self.test_file,
            "--cov=src.streaming.producers",
            "--cov-report=html:htmlcov/report",
            "--cov-report=term",
            "--html=test_report.html",
            "--self-contained-html",
            "--json-report",
            "--json-report-file=test_report.json",
            "-v",
        ]

        if not self.check_kafka_availability():
            cmd.extend(["-m", "not integration"])

        try:
            subprocess.run(cmd, cwd=self.base_dir)
            logger.info("Test report generated: test_report.html")
            logger.info("Coverage report generated: htmlcov/report/index.html")
        except Exception as e:
            logger.error(f"Report generation failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer Test Runner")
    parser.add_argument(
        "--mode",
        choices=[
            "unit",
            "integration",
            "performance",
            "schema",
            "error",
            "config",
            "all",
            "quick",
            "report",
        ],
        default="unit",
        help="Test mode to run (default: unit)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    runner = KafkaProducerTestRunner()

    # Execute based on mode
    success = False

    if args.mode == "unit":
        success = runner.run_unit_tests()
    elif args.mode == "integration":
        success = runner.run_integration_tests()
    elif args.mode == "performance":
        success = runner.run_performance_tests()
    elif args.mode == "schema":
        success = runner.run_schema_tests()
    elif args.mode == "error":
        success = runner.run_error_handling_tests()
    elif args.mode == "config":
        success = runner.run_config_tests()
    elif args.mode == "all":
        success = runner.run_all_tests()
    elif args.mode == "quick":
        success = runner.run_quick_tests()
    elif args.mode == "report":
        runner.generate_test_report()
        success = True

    if success:
        logger.info("Tests completed successfully!")
        return 0
    else:
        logger.error("Tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
