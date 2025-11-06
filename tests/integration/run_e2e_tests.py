#!/usr/bin/env python3
"""
Master Test Runner for End-to-End Integration Tests

This script provides a unified interface for running all types of integration tests
including pipeline validation, performance benchmarks, and error scenario testing.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add the tests directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from test_e2e_pipeline import E2EPipelineTestSuite, run_full_test_suite, run_single_test
from error_scenario_tests import ErrorScenarioTestSuite
from performance_monitor import PerformanceMonitor, BenchmarkRunner
from infrastructure_manager import InfrastructureManager
from e2e_config import E2E_CONFIG


class MasterTestRunner:
    """Master test runner that orchestrates all integration tests"""

    def __init__(self, config_file: Optional[str] = None):
        self.config = E2E_CONFIG
        if config_file:
            # Load custom configuration
            pass

        self.logger = logging.getLogger(__name__)
        self.setup_logging()

        # Initialize components
        self.pipeline_suite = E2EPipelineTestSuite()
        self.error_suite = ErrorScenarioTestSuite()
        self.performance_monitor = PerformanceMonitor()
        self.benchmark_runner = BenchmarkRunner(self.performance_monitor)
        self.infrastructure = InfrastructureManager(self.config)

        # Results tracking
        self.test_results = {}
        self.start_time = None
        self.end_time = None

    def setup_logging(self):
        """Setup comprehensive logging"""

        log_level = os.getenv("E2E_LOG_LEVEL", "INFO")

        # Create logs directory
        log_dir = Path("tests/integration/logs")
        log_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging configuration
        logging.basicConfig(
            level=getattr(logging, log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(
                    log_dir
                    / f"master_test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                ),
            ],
        )

        # Reduce noise from external libraries
        logging.getLogger("docker").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("kafka").setLevel(logging.WARNING)

    async def run_complete_test_suite(
        self,
        include_pipeline: bool = True,
        include_performance: bool = True,
        include_error_scenarios: bool = True,
        include_benchmarks: bool = False,
        record_count: int = 1000,
        output_dir: str = "tests/integration/reports",
    ) -> Dict[str, Any]:
        """Run the complete integration test suite"""

        self.logger.info("Starting Complete Integration Test Suite")
        self.start_time = datetime.now()

        try:
            # Ensure output directory exists
            Path(output_dir).mkdir(parents=True, exist_ok=True)

            # 1. Setup infrastructure
            await self._setup_infrastructure()

            # 2. Run pipeline tests
            if include_pipeline:
                await self._run_pipeline_tests(record_count, output_dir)

            # 3. Run performance tests
            if include_performance:
                await self._run_performance_tests(record_count, output_dir)

            # 4. Run error scenario tests
            if include_error_scenarios:
                await self._run_error_scenario_tests(output_dir)

            # 5. Run benchmarks
            if include_benchmarks:
                await self._run_benchmark_tests(output_dir)

            # 6. Generate comprehensive report
            await self._generate_master_report(output_dir)

            self.end_time = datetime.now()
            total_duration = (self.end_time - self.start_time).total_seconds()

            self.logger.info(
                f"Complete test suite finished in {total_duration:.2f} seconds"
            )

            return {
                "success": True,
                "duration_seconds": total_duration,
                "results": self.test_results,
                "report_path": str(Path(output_dir) / "master_test_report.json"),
            }

        except Exception as e:
            self.end_time = datetime.now()
            total_duration = (
                (self.end_time - self.start_time).total_seconds()
                if self.start_time
                else 0
            )

            self.logger.error(
                f"Test suite failed after {total_duration:.2f} seconds: {e}"
            )

            return {
                "success": False,
                "duration_seconds": total_duration,
                "error": str(e),
                "results": self.test_results,
            }

        finally:
            # Cleanup infrastructure
            await self._cleanup_infrastructure()

    async def _setup_infrastructure(self):
        """Setup test infrastructure"""

        self.logger.info("Setting up test infrastructure")

        try:
            await self.infrastructure.start_services()
            await self.infrastructure.wait_for_readiness()
            await self.infrastructure.setup_databases()
            await self.infrastructure.setup_kafka_topics()
            await self.infrastructure.setup_s3_buckets()

            self.logger.info("Infrastructure setup complete")

        except Exception as e:
            self.logger.error(f"Infrastructure setup failed: {e}")
            raise

    async def _run_pipeline_tests(self, record_count: int, output_dir: str):
        """Run pipeline validation tests"""

        self.logger.info("Running Pipeline Tests")

        try:
            # Run all pipeline test scenarios
            pipeline_results = await run_full_test_suite()

            self.test_results["pipeline_tests"] = {
                "success": all(result.success for result in pipeline_results.values()),
                "total_tests": len(pipeline_results),
                "passed_tests": sum(
                    1 for result in pipeline_results.values() if result.success
                ),
                "results": {
                    name: result.__dict__ for name, result in pipeline_results.items()
                },
            }

            # Save detailed pipeline report
            pipeline_report_path = Path(output_dir) / "pipeline_test_report.json"
            with open(pipeline_report_path, "w") as f:
                json.dump(self.test_results["pipeline_tests"], f, indent=2, default=str)

            success_count = self.test_results["pipeline_tests"]["passed_tests"]
            total_count = self.test_results["pipeline_tests"]["total_tests"]

            self.logger.info(
                f"Pipeline tests complete: {success_count}/{total_count} passed"
            )

        except Exception as e:
            self.logger.error(f"Pipeline tests failed: {e}")
            self.test_results["pipeline_tests"] = {"success": False, "error": str(e)}

    async def _run_performance_tests(self, record_count: int, output_dir: str):
        """Run performance monitoring tests"""

        self.logger.info("Running Performance Tests")

        try:
            # Capture baseline performance
            baseline = self.performance_monitor.capture_baseline()

            # Run performance test scenario
            performance_result = await run_single_test(
                "performance", record_count=record_count
            )

            # Analyze performance
            if performance_result.success and performance_result.performance_metrics:
                performance_analysis = (
                    self.performance_monitor.analyze_performance_trends([])
                )

                self.test_results["performance_tests"] = {
                    "success": True,
                    "baseline": baseline.to_dict(),
                    "test_result": performance_result.__dict__,
                    "analysis": performance_analysis,
                }
            else:
                self.test_results["performance_tests"] = {
                    "success": False,
                    "error": performance_result.error_details
                    or "Performance test failed",
                }

            # Save performance report
            performance_report_path = Path(output_dir) / "performance_test_report.json"
            with open(performance_report_path, "w") as f:
                json.dump(
                    self.test_results["performance_tests"], f, indent=2, default=str
                )

            self.logger.info("Performance tests complete")

        except Exception as e:
            self.logger.error(f"Performance tests failed: {e}")
            self.test_results["performance_tests"] = {"success": False, "error": str(e)}

    async def _run_error_scenario_tests(self, output_dir: str):
        """Run error scenario and recovery tests"""

        self.logger.info("Running Error Scenario Tests")

        try:
            # Run all error scenarios
            error_results = await self.error_suite.run_all_error_scenarios()

            self.test_results["error_scenarios"] = {
                "success": all(result.test_passed for result in error_results.values()),
                "total_scenarios": len(error_results),
                "passed_scenarios": sum(
                    1 for result in error_results.values() if result.test_passed
                ),
                "results": {
                    name: result.to_dict() for name, result in error_results.items()
                },
            }

            # Generate error test report
            error_report_path = Path(output_dir) / "error_scenario_report.json"
            self.error_suite.generate_error_test_report(
                error_results, str(error_report_path)
            )

            success_count = self.test_results["error_scenarios"]["passed_scenarios"]
            total_count = self.test_results["error_scenarios"]["total_scenarios"]

            self.logger.info(
                f"Error scenario tests complete: {success_count}/{total_count} passed"
            )

        except Exception as e:
            self.logger.error(f"Error scenario tests failed: {e}")
            self.test_results["error_scenarios"] = {"success": False, "error": str(e)}

    async def _run_benchmark_tests(self, output_dir: str):
        """Run performance benchmark tests"""

        self.logger.info("Running Benchmark Tests")

        try:
            # Define benchmark workloads
            workload_sizes = [1000, 5000, 10000, 25000, 50000]

            # Run throughput benchmarks for key components
            benchmark_results = {}

            # Kafka throughput benchmark
            kafka_benchmark = await self.benchmark_runner.run_throughput_benchmark(
                "kafka_streaming", workload_sizes
            )
            benchmark_results["kafka"] = kafka_benchmark

            # Spark throughput benchmark
            spark_benchmark = await self.benchmark_runner.run_throughput_benchmark(
                "spark_processing", workload_sizes
            )
            benchmark_results["spark"] = spark_benchmark

            # Run scalability test
            scalability_result = await self.benchmark_runner.run_scalability_test(
                "complete_pipeline", max_workload=100000, step_size=10000
            )
            benchmark_results["scalability"] = scalability_result

            self.test_results["benchmarks"] = {
                "success": True,
                "results": benchmark_results,
            }

            # Save benchmark report
            benchmark_report_path = Path(output_dir) / "benchmark_report.json"
            with open(benchmark_report_path, "w") as f:
                json.dump(self.test_results["benchmarks"], f, indent=2, default=str)

            self.logger.info("Benchmark tests complete")

        except Exception as e:
            self.logger.error(f"Benchmark tests failed: {e}")
            self.test_results["benchmarks"] = {"success": False, "error": str(e)}

    async def _generate_master_report(self, output_dir: str):
        """Generate comprehensive master test report"""

        self.logger.info("Generating master test report")

        try:
            # Calculate overall statistics
            total_tests = 0
            passed_tests = 0

            for test_category, results in self.test_results.items():
                if "total_tests" in results:
                    total_tests += results["total_tests"]
                    passed_tests += results.get("passed_tests", 0)
                elif "total_scenarios" in results:
                    total_tests += results["total_scenarios"]
                    passed_tests += results.get("passed_scenarios", 0)
                elif results.get("success"):
                    total_tests += 1
                    passed_tests += 1
                else:
                    total_tests += 1

            overall_success = all(
                results.get("success", False) for results in self.test_results.values()
            )

            success_rate = passed_tests / total_tests if total_tests > 0 else 0

            # Create master report
            master_report = {
                "test_run_metadata": {
                    "start_time": (
                        self.start_time.isoformat() if self.start_time else None
                    ),
                    "end_time": self.end_time.isoformat() if self.end_time else None,
                    "duration_seconds": (
                        (self.end_time - self.start_time).total_seconds()
                        if self.start_time and self.end_time
                        else 0
                    ),
                    "test_environment": "integration",
                    "runner_version": "1.0.0",
                },
                "overall_results": {
                    "success": overall_success,
                    "total_tests": total_tests,
                    "passed_tests": passed_tests,
                    "failed_tests": total_tests - passed_tests,
                    "success_rate": success_rate,
                },
                "test_categories": self.test_results,
                "configuration": self.config.to_dict(),
                "recommendations": self._generate_recommendations(),
            }

            # Save master report
            master_report_path = Path(output_dir) / "master_test_report.json"
            with open(master_report_path, "w") as f:
                json.dump(master_report, f, indent=2, default=str)

            # Generate summary
            self._print_test_summary(master_report)

            self.logger.info(f"Master report saved to: {master_report_path}")

        except Exception as e:
            self.logger.error(f"Failed to generate master report: {e}")

    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on test results"""

        recommendations = []

        # Check pipeline test results
        if "pipeline_tests" in self.test_results:
            pipeline_results = self.test_results["pipeline_tests"]
            if not pipeline_results.get("success"):
                recommendations.append(
                    "Review pipeline test failures and fix critical issues"
                )

            success_rate = pipeline_results.get("passed_tests", 0) / max(
                1, pipeline_results.get("total_tests", 1)
            )
            if success_rate < 0.9:
                recommendations.append(
                    "Pipeline success rate is below 90% - investigate recurring failures"
                )

        # Check performance results
        if "performance_tests" in self.test_results:
            perf_results = self.test_results["performance_tests"]
            if not perf_results.get("success"):
                recommendations.append(
                    "Performance tests failed - check resource allocation and optimize bottlenecks"
                )

        # Check error scenario results
        if "error_scenarios" in self.test_results:
            error_results = self.test_results["error_scenarios"]
            success_rate = error_results.get("passed_scenarios", 0) / max(
                1, error_results.get("total_scenarios", 1)
            )
            if success_rate < 0.7:
                recommendations.append(
                    "Error recovery success rate is below 70% - improve fault tolerance"
                )

        # General recommendations
        if not recommendations:
            recommendations.append(
                "All tests passed successfully - consider adding more edge case scenarios"
            )

        return recommendations

    def _print_test_summary(self, master_report: Dict[str, Any]):
        """Print a formatted test summary"""

        print("\n" + "=" * 80)
        print("INTEGRATION TEST SUITE SUMMARY")
        print("=" * 80)

        overall = master_report["overall_results"]
        metadata = master_report["test_run_metadata"]

        # Overall results
        status = "PASSED" if overall["success"] else "FAILED"
        print(f"\nOverall Status: {'PASSED' if overall['success'] else 'FAILED'}")
        print(f"Success Rate: {overall['success_rate']:.1%}")
        print(f"Duration: {metadata['duration_seconds']:.1f} seconds")
        print(f"Total Tests: {overall['total_tests']}")
        print(f"Passed: {overall['passed_tests']}")
        print(f"Failed: {overall['failed_tests']}")

        # Category breakdown
        print("\nTest Category Results:")
        for category, results in master_report["test_categories"].items():
            category_status = "PASSED" if results.get("success") else "FAILED"
            category_name = category.replace("_", " ").title()

            if "total_tests" in results:
                print(
                    f"  {category_status} {category_name}: {results['passed_tests']}/{results['total_tests']}"
                )
            elif "total_scenarios" in results:
                print(
                    f"  {category_status} {category_name}: {results['passed_scenarios']}/{results['total_scenarios']}"
                )
            else:
                status = "PASSED" if results.get("success") else "FAILED"
                print(f"  {category_status} {category_name}: {status}")

        # Recommendations
        if master_report["recommendations"]:
            print("\nRecommendations:")
            for i, rec in enumerate(master_report["recommendations"], 1):
                print(f"  {i}. {rec}")

        print("\n" + "=" * 80)

    async def _cleanup_infrastructure(self):
        """Clean up test infrastructure"""

        self.logger.info("Cleaning up infrastructure")

        try:
            if self.config.test_environment.cleanup_containers_on_success:
                await self.infrastructure.cleanup()
                self.logger.info("Infrastructure cleanup complete")
            else:
                self.logger.info(
                    "Infrastructure cleanup skipped (configured to preserve)"
                )

        except Exception as e:
            self.logger.warning(f"Infrastructure cleanup failed: {e}")


async def main():
    """Main entry point for the master test runner"""

    parser = argparse.ArgumentParser(
        description="Master Integration Test Runner for Sales Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete test suite
  python run_e2e_tests.py --all

  # Run only pipeline tests
  python run_e2e_tests.py --pipeline-only --record-count 5000

  # Run performance and benchmarks
  python run_e2e_tests.py --performance --benchmarks

  # Run with custom configuration
  python run_e2e_tests.py --all --config custom_config.yml
        """,
    )

    # Test selection arguments
    parser.add_argument("--all", action="store_true", help="Run all integration tests")
    parser.add_argument(
        "--pipeline-only",
        action="store_true",
        help="Run only pipeline validation tests",
    )
    parser.add_argument(
        "--performance", action="store_true", help="Run performance tests"
    )
    parser.add_argument(
        "--error-scenarios", action="store_true", help="Run error scenario tests"
    )
    parser.add_argument("--benchmarks", action="store_true", help="Run benchmark tests")

    # Configuration arguments
    parser.add_argument("--config", type=str, help="Custom configuration file")
    parser.add_argument(
        "--record-count",
        type=int,
        default=1000,
        help="Number of records for test data generation",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="tests/integration/reports",
        help="Output directory for test reports",
    )

    # Execution arguments
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--no-cleanup", action="store_true", help="Skip infrastructure cleanup"
    )

    args = parser.parse_args()

    # Set log level
    os.environ["E2E_LOG_LEVEL"] = args.log_level

    # Configure cleanup behavior
    if args.no_cleanup:
        os.environ["E2E_CLEANUP_ON_SUCCESS"] = "false"

    # Determine what tests to run
    if args.all:
        include_pipeline = True
        include_performance = True
        include_error_scenarios = True
        include_benchmarks = True
    else:
        include_pipeline = args.pipeline_only or args.all
        include_performance = args.performance or args.all
        include_error_scenarios = args.error_scenarios or args.all
        include_benchmarks = args.benchmarks or args.all

        # If nothing specified, run pipeline tests by default
        if not any(
            [
                include_pipeline,
                include_performance,
                include_error_scenarios,
                include_benchmarks,
            ]
        ):
            include_pipeline = True

    # Initialize and run test runner
    runner = MasterTestRunner(args.config)

    try:
        result = await runner.run_complete_test_suite(
            include_pipeline=include_pipeline,
            include_performance=include_performance,
            include_error_scenarios=include_error_scenarios,
            include_benchmarks=include_benchmarks,
            record_count=args.record_count,
            output_dir=args.output_dir,
        )

        # Exit with appropriate code
        exit_code = 0 if result["success"] else 1
        print(f"\nTest runner finished with exit code: {exit_code}")

        if result.get("report_path"):
            print(f"Full report available at: {result['report_path']}")

        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nTest runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
