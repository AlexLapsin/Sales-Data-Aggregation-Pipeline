#!/usr/bin/env python3
"""
Test Runner Script for Sales Data Pipeline

This script provides a convenient way to run different categories of tests
with proper setup validation and clear reporting.
"""

import subprocess
import sys
import time
import argparse
import os
from pathlib import Path
from typing import List, Dict, Optional
import logging
import json


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestRunner:
    """Manages test execution and reporting for the sales data pipeline"""

    def __init__(self):
        self.project_root = Path(
            __file__
        ).parent.parent.parent  # Go up to actual project root
        self.tests_dir = self.project_root / "tests"
        self.test_categories = {
            "config": {
                "file": "tests/test_config_validation.py",
                "description": "Configuration and environment validation",
                "prerequisites": ["env_file"],
                "estimated_time": "3-5 minutes",
            },
            "docker": {
                "file": "tests/test_docker_health.py",
                "description": "Docker container health and connectivity",
                "prerequisites": ["docker_running", "containers_started"],
                "estimated_time": "2-3 minutes",
            },
            "readiness": {
                "file": "tests/test_service_readiness.py",
                "description": "Service startup and readiness checks",
                "prerequisites": ["docker_running", "containers_started"],
                "estimated_time": "5-10 minutes",
            },
            "communication": {
                "file": "tests/test_inter_service_communication.py",
                "description": "Inter-service communication validation",
                "prerequisites": [
                    "docker_running",
                    "containers_started",
                    "services_ready",
                ],
                "estimated_time": "3-5 minutes",
            },
        }

    def check_prerequisites(self, category: str) -> Dict[str, bool]:
        """Check if prerequisites for a test category are met"""
        prereq_checks = {
            "env_file": self._check_env_file,
            "docker_running": self._check_docker_running,
            "containers_started": self._check_containers_started,
            "services_ready": self._check_services_ready,
            "kafka_running": self._check_kafka_running,
        }

        category_info = self.test_categories.get(category, {})
        prerequisites = category_info.get("prerequisites", [])

        results = {}
        for prereq in prerequisites:
            if prereq in prereq_checks:
                results[prereq] = prereq_checks[prereq]()
            else:
                results[prereq] = False
                logger.warning(f"Unknown prerequisite: {prereq}")

        return results

    def _check_env_file(self) -> bool:
        """Check if .env file exists"""
        env_file = self.project_root / ".env"
        return env_file.exists()

    def _check_docker_running(self) -> bool:
        """Check if Docker is running"""
        try:
            result = subprocess.run(
                ["docker", "info"], capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _check_containers_started(self) -> bool:
        """Check if Docker containers are started"""
        try:
            result = subprocess.run(
                ["docker-compose", "ps", "-q"],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=self.project_root,
            )
            container_ids = result.stdout.strip().split("\n")
            return len(container_ids) > 0 and container_ids[0] != ""
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    def _check_services_ready(self) -> bool:
        """Check if core services are ready"""
        try:
            import requests
            import psycopg2

            # Check Airflow
            airflow_response = requests.get("http://localhost:8080/health", timeout=5)
            airflow_ready = airflow_response.status_code == 200

            # Check PostgreSQL
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port="5432",
                    database="airflow",
                    user="airflow",
                    password="airflow",
                    connect_timeout=5,
                )
                conn.close()
                postgres_ready = True
            except:
                postgres_ready = False

            return airflow_ready and postgres_ready

        except:
            return False

    def _check_kafka_running(self) -> bool:
        """Check if Kafka is running"""
        try:
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"], request_timeout_ms=5000
            )
            producer.close()
            return True
        except:
            return False

    def run_test_category(
        self, category: str, verbose: bool = True, coverage: bool = False
    ) -> Dict[str, any]:
        """Run tests for a specific category"""
        if category not in self.test_categories:
            raise ValueError(f"Unknown test category: {category}")

        category_info = self.test_categories[category]
        test_file = self.tests_dir / category_info["file"]

        if not test_file.exists():
            raise FileNotFoundError(f"Test file not found: {test_file}")

        # Check prerequisites
        prereq_results = self.check_prerequisites(category)
        failed_prereqs = [k for k, v in prereq_results.items() if not v]

        if failed_prereqs:
            logger.warning(f"Prerequisites not met for {category}: {failed_prereqs}")
            return {
                "category": category,
                "success": False,
                "error": f"Prerequisites not met: {failed_prereqs}",
                "prerequisites": prereq_results,
            }

        # Build pytest command
        cmd = ["python", "-m", "pytest", str(test_file)]

        if verbose:
            cmd.append("-v")

        if coverage:
            cmd.extend(["--cov=.", "--cov-report=term-missing"])

        logger.info(f"Running {category} tests ({category_info['description']})...")
        logger.info(f"Estimated time: {category_info['estimated_time']}")

        # Run tests
        start_time = time.time()
        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )

            end_time = time.time()
            duration = end_time - start_time

            return {
                "category": category,
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "duration": duration,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "prerequisites": prereq_results,
            }

        except subprocess.TimeoutExpired:
            return {
                "category": category,
                "success": False,
                "error": "Test execution timed out",
                "duration": 600,
                "prerequisites": prereq_results,
            }

    def run_all_tests(
        self,
        exclude_categories: Optional[List[str]] = None,
        verbose: bool = True,
        coverage: bool = False,
    ) -> Dict[str, any]:
        """Run all test categories"""
        exclude_categories = exclude_categories or []

        results = {}
        total_start_time = time.time()

        for category in self.test_categories:
            if category in exclude_categories:
                logger.info(f"Skipping {category} tests (excluded)")
                continue

            result = self.run_test_category(category, verbose, coverage)
            results[category] = result

            # Print immediate feedback
            if result["success"]:
                logger.info(
                    f"✓ {category} tests passed ({result.get('duration', 0):.1f}s)"
                )
            else:
                logger.error(f"✗ {category} tests failed")
                if "error" in result:
                    logger.error(f"  Error: {result['error']}")

        total_duration = time.time() - total_start_time

        return {
            "results": results,
            "total_duration": total_duration,
            "summary": self._generate_summary(results),
        }

    def _generate_summary(self, results: Dict[str, any]) -> Dict[str, any]:
        """Generate test summary"""
        total_tests = len(results)
        passed_tests = sum(1 for r in results.values() if r.get("success", False))
        failed_tests = total_tests - passed_tests

        return {
            "total_categories": total_tests,
            "passed_categories": passed_tests,
            "failed_categories": failed_tests,
            "success_rate": (passed_tests / total_tests * 100)
            if total_tests > 0
            else 0,
            "failed_categories_list": [
                category
                for category, result in results.items()
                if not result.get("success", False)
            ],
        }

    def print_summary_report(self, results: Dict[str, any]) -> None:
        """Print a formatted summary report"""
        print("\n" + "=" * 60)
        print("SALES DATA PIPELINE TEST SUMMARY")
        print("=" * 60)

        summary = results["summary"]
        print(f"Total test categories: {summary['total_categories']}")
        print(f"Passed: {summary['passed_categories']}")
        print(f"Failed: {summary['failed_categories']}")
        print(f"Success rate: {summary['success_rate']:.1f}%")
        print(f"Total execution time: {results['total_duration']:.1f} seconds")

        if summary["failed_categories"]:
            print(
                f"\nFailed categories: {', '.join(summary['failed_categories_list'])}"
            )

        print("\nDetailed Results:")
        print("-" * 40)

        for category, result in results["results"].items():
            status = "PASS" if result.get("success", False) else "FAIL"
            duration = result.get("duration", 0)
            print(f"{category:15} | {status:4} | {duration:6.1f}s")

            if not result.get("success", False) and "error" in result:
                print(f"                   Error: {result['error']}")

        print("\n" + "=" * 60)

    def setup_environment(self) -> bool:
        """Help user set up the testing environment"""
        print("Setting up testing environment...")

        # Check .env file
        if not self._check_env_file():
            print("ERROR: .env file not found")
            print("   Run: cp .env.example .env")
            print("   Then edit .env with your configuration")
            return False

        print("SUCCESS: .env file exists")

        # Check Docker
        if not self._check_docker_running():
            print("ERROR: Docker is not running")
            print("   Please start Docker Desktop")
            return False

        print("SUCCESS: Docker is running")

        # Check if containers are built
        try:
            result = subprocess.run(
                ["docker", "images", "sales-pipeline:latest", "-q"],
                capture_output=True,
                text=True,
            )
            if not result.stdout.strip():
                print("ERROR: Docker images not built")
                print("   Run: docker build -t sales-pipeline:latest .")
                return False
        except:
            return False

        print("SUCCESS: Docker images are built")

        print("\nEnvironment setup complete! You can now run tests.")
        return True


def main():
    parser = argparse.ArgumentParser(description="Sales Data Pipeline Test Runner")
    parser.add_argument(
        "category",
        nargs="?",
        choices=list(TestRunner().test_categories.keys()) + ["all"],
        help="Test category to run (or 'all' for all categories)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--coverage", "-c", action="store_true", help="Include coverage report"
    )
    parser.add_argument(
        "--exclude", nargs="*", help="Categories to exclude (when running all)"
    )
    parser.add_argument(
        "--setup", action="store_true", help="Check and help setup testing environment"
    )
    parser.add_argument(
        "--list", "-l", action="store_true", help="List available test categories"
    )

    args = parser.parse_args()

    runner = TestRunner()

    if args.list:
        print("Available test categories:")
        for category, info in runner.test_categories.items():
            print(f"  {category:15} - {info['description']}")
            print(f"                   Estimated time: {info['estimated_time']}")
        return

    if args.setup:
        runner.setup_environment()
        return

    if not args.category:
        print("No test category specified. Use --list to see available categories.")
        print("Example: python run_tests.py config")
        print("         python run_tests.py all")
        return

    try:
        if args.category == "all":
            results = runner.run_all_tests(
                exclude_categories=args.exclude,
                verbose=args.verbose,
                coverage=args.coverage,
            )
            runner.print_summary_report(results)

            # Exit with error code if any tests failed
            if results["summary"]["failed_categories"] > 0:
                sys.exit(1)

        else:
            result = runner.run_test_category(
                args.category, verbose=args.verbose, coverage=args.coverage
            )

            if result["success"]:
                print(f"\n✓ {args.category} tests passed!")
            else:
                print(f"\n✗ {args.category} tests failed!")
                if "error" in result:
                    print(f"Error: {result['error']}")
                if "stderr" in result and result["stderr"]:
                    print(f"Details: {result['stderr']}")
                sys.exit(1)

    except KeyboardInterrupt:
        print("\nTest execution interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
