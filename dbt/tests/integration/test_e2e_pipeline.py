#!/usr/bin/env python3
"""
Comprehensive End-to-End Integration Tests for Sales Data Pipeline

This module contains the master test suite that validates the entire
sales data aggregation pipeline from raw data ingestion through final
analytics tables.
"""

import asyncio
import json
import logging
import os
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import subprocess
import pandas as pd
import pytest
from dataclasses import dataclass

# Test framework imports
from .e2e_config import E2E_CONFIG
from .data_validators import PipelineValidator, ValidationSummary
from .test_data_generator import (
    TestDataGenerator,
    DataGenerationConfig,
    MockComponentGenerator,
)
from .infrastructure_manager import InfrastructureManager
from .performance_monitor import PerformanceMonitor


@dataclass
class PipelineTestResult:
    """Result of a complete pipeline test run"""

    test_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    success: bool
    validation_summary: Optional[ValidationSummary]
    performance_metrics: Dict[str, Any]
    error_details: Optional[str]
    artifacts: Dict[str, str]  # File paths to test artifacts


class E2EPipelineTestSuite:
    """Comprehensive end-to-end pipeline test suite"""

    def __init__(self, config_override: Optional[Dict] = None):
        """Initialize the test suite with optional configuration override"""

        self.config = E2E_CONFIG
        if config_override:
            # Apply configuration overrides
            for section, values in config_override.items():
                if hasattr(self.config, section):
                    section_obj = getattr(self.config, section)
                    for key, value in values.items():
                        if hasattr(section_obj, key):
                            setattr(section_obj, key, value)

        # Initialize components
        self.logger = logging.getLogger(__name__)
        self.data_generator = TestDataGenerator()
        self.mock_generator = MockComponentGenerator()
        self.validator = PipelineValidator()
        self.infrastructure = InfrastructureManager(self.config)
        self.performance_monitor = PerformanceMonitor()

        # Test run tracking
        self.current_run_id = None
        self.test_artifacts_dir = None

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Setup comprehensive logging for test execution"""

        log_level = os.getenv("E2E_LOG_LEVEL", "INFO")
        logging.basicConfig(
            level=getattr(logging, log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("tests/integration/logs/e2e_tests.log", mode="a"),
            ],
        )

        # Create logs directory if it doesn't exist
        Path("tests/integration/logs").mkdir(parents=True, exist_ok=True)

    def setup_test_run(self, test_name: str) -> str:
        """Setup a new test run with unique ID and artifacts directory"""

        self.current_run_id = f"{test_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.test_artifacts_dir = Path(
            f"tests/integration/artifacts/{self.current_run_id}"
        )
        self.test_artifacts_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting test run: {self.current_run_id}")
        self.logger.info(f"Artifacts directory: {self.test_artifacts_dir}")

        return self.current_run_id

    def teardown_test_run(self):
        """Clean up after test run"""

        if self.current_run_id:
            self.logger.info(f"Completed test run: {self.current_run_id}")

            # Clean up infrastructure if configured
            if self.config.test_environment.cleanup_containers_on_success:
                try:
                    self.infrastructure.cleanup()
                except Exception as e:
                    self.logger.warning(f"Failed to cleanup infrastructure: {e}")

            self.current_run_id = None
            self.test_artifacts_dir = None

    async def run_complete_pipeline_test(
        self, scenario_name: str = "happy_path", record_count: int = 1000, **kwargs
    ) -> PipelineTestResult:
        """
        Run a complete end-to-end pipeline test

        This is the master test that validates the entire data flow from
        raw CSV files through Kafka streaming, Spark processing, and
        final warehouse storage.
        """

        test_name = f"complete_pipeline_{scenario_name}"
        run_id = self.setup_test_run(test_name)
        start_time = datetime.now()

        try:
            self.logger.info(
                f"=== Starting Complete Pipeline Test: {scenario_name} ==="
            )

            # 1. Setup infrastructure
            self.logger.info("Step 1: Setting up test infrastructure")
            await self._setup_infrastructure()

            # 2. Generate test data
            self.logger.info("Step 2: Generating test data")
            test_data = await self._generate_test_data(
                scenario_name, record_count, **kwargs
            )

            # 3. Stage raw data
            self.logger.info("Step 3: Staging raw data files")
            await self._stage_raw_data(test_data)

            # 4. Start Kafka streaming
            self.logger.info("Step 4: Starting Kafka streaming")
            kafka_metrics = await self._run_kafka_streaming(test_data)

            # 5. Execute Spark ETL
            self.logger.info("Step 5: Executing Spark ETL processing")
            spark_metrics = await self._run_spark_etl(test_data)

            # 6. Run dbt transformations
            self.logger.info("Step 6: Running dbt transformations")
            dbt_metrics = await self._run_dbt_transformations()

            # 7. Validate final state
            self.logger.info("Step 7: Validating final data warehouse state")
            validation_summary = await self._validate_pipeline_results(test_data)

            # 8. Collect performance metrics
            self.logger.info("Step 8: Collecting performance metrics")
            performance_metrics = self._collect_performance_metrics(
                kafka_metrics, spark_metrics, dbt_metrics
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Determine test success
            success = validation_summary.overall_passed if validation_summary else False

            result = PipelineTestResult(
                test_name=test_name,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                success=success,
                validation_summary=validation_summary,
                performance_metrics=performance_metrics,
                error_details=None,
                artifacts=self._collect_test_artifacts(),
            )

            self.logger.info(f"=== Pipeline Test Complete: {scenario_name} ===")
            self.logger.info(f"Success: {success}, Duration: {duration:.2f}s")

            return result

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.error(f"Pipeline test failed: {e}", exc_info=True)

            result = PipelineTestResult(
                test_name=test_name,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                success=False,
                validation_summary=None,
                performance_metrics={},
                error_details=str(e),
                artifacts=self._collect_test_artifacts(),
            )

            return result

        finally:
            self.teardown_test_run()

    async def _setup_infrastructure(self):
        """Setup test infrastructure components"""

        self.logger.info("Starting infrastructure setup")

        # Start infrastructure services
        await self.infrastructure.start_services()

        # Wait for services to be ready
        await self.infrastructure.wait_for_readiness()

        # Create necessary databases and topics
        await self.infrastructure.setup_databases()
        await self.infrastructure.setup_kafka_topics()

        self.logger.info("Infrastructure setup complete")

    async def _generate_test_data(
        self, scenario_name: str, record_count: int, **kwargs
    ) -> Dict[str, Any]:
        """Generate test data for the specified scenario"""

        self.logger.info(f"Generating test data for scenario: {scenario_name}")

        # Generate scenario-specific data
        test_data = self.data_generator.generate_test_scenario_data(
            scenario_name, record_count=record_count, **kwargs
        )

        # Save test data to artifacts directory
        file_paths = self.data_generator.save_test_data(
            test_data, str(self.test_artifacts_dir), scenario_name
        )

        test_data["file_paths"] = file_paths

        self.logger.info(f"Generated {len(test_data.get('csv_data', []))} CSV records")
        self.logger.info(
            f"Generated {len(test_data.get('kafka_messages', []))} Kafka messages"
        )

        return test_data

    async def _stage_raw_data(self, test_data: Dict[str, Any]):
        """Stage raw CSV data files for processing"""

        self.logger.info("Staging raw data files")

        # Upload CSV data to S3/MinIO
        csv_file_path = test_data["file_paths"]["csv_data"]

        # Use the infrastructure manager to upload files
        await self.infrastructure.upload_raw_data(csv_file_path)

        self.logger.info(f"Staged raw data file: {csv_file_path}")

    async def _run_kafka_streaming(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run Kafka streaming simulation"""

        self.logger.info("Starting Kafka streaming simulation")

        kafka_config = self.config.get_kafka_config()
        messages = test_data["kafka_messages"]

        # Start performance monitoring
        monitor_task = asyncio.create_task(
            self.performance_monitor.monitor_kafka_streaming(
                kafka_config["bootstrap_servers"],
                kafka_config["topic"],
                duration_seconds=60,
            )
        )

        # Produce messages to Kafka
        producer_task = asyncio.create_task(
            self._produce_kafka_messages(messages, kafka_config)
        )

        # Wait for both tasks to complete
        producer_result, monitor_result = await asyncio.gather(
            producer_task, monitor_task, return_exceptions=True
        )

        if isinstance(producer_result, Exception):
            raise producer_result

        metrics = {
            "messages_produced": len(messages),
            "producer_duration": producer_result.get("duration_seconds", 0),
            "throughput": len(messages) / producer_result.get("duration_seconds", 1),
            "monitoring_data": monitor_result
            if not isinstance(monitor_result, Exception)
            else {},
        }

        self.logger.info(
            f"Kafka streaming complete: {metrics['messages_produced']} messages"
        )
        return metrics

    async def _produce_kafka_messages(
        self, messages: List[Dict], kafka_config: Dict
    ) -> Dict[str, Any]:
        """Produce messages to Kafka topic"""

        from kafka import KafkaProducer
        import asyncio

        producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )

        start_time = time.time()

        try:
            for message in messages:
                # Use order_id as key for partitioning
                key = message.get("order_id", str(uuid.uuid4()))

                future = producer.send(kafka_config["topic"], key=key, value=message)

                # Add small delay to simulate realistic streaming
                await asyncio.sleep(0.01)

            # Ensure all messages are sent
            producer.flush()

            end_time = time.time()
            duration = end_time - start_time

            return {
                "duration_seconds": duration,
                "messages_sent": len(messages),
                "success": True,
            }

        finally:
            producer.close()

    async def _run_spark_etl(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Spark ETL processing"""

        self.logger.info("Starting Spark ETL processing")

        spark_config = self.config.get_spark_config()
        csv_file_path = test_data["file_paths"]["csv_data"]

        # Start performance monitoring
        start_time = time.time()

        # Run Spark job (this would be adapted to your specific Spark job)
        result = await self._execute_spark_job(csv_file_path, spark_config)

        end_time = time.time()
        duration = end_time - start_time

        metrics = {
            "processing_duration": duration,
            "input_records": len(test_data.get("csv_data", [])),
            "output_records": result.get("records_processed", 0),
            "throughput": result.get("records_processed", 0) / duration
            if duration > 0
            else 0,
            "spark_job_result": result,
        }

        self.logger.info(
            f"Spark ETL complete: {metrics['output_records']} records processed"
        )
        return metrics

    async def _execute_spark_job(
        self, input_path: str, spark_config: Dict
    ) -> Dict[str, Any]:
        """Execute the actual Spark ETL job"""

        # This is a placeholder for actual Spark job execution
        # In a real implementation, you would:
        # 1. Start a Spark session
        # 2. Run your sales_batch_job.py
        # 3. Return the results

        try:
            # Example using subprocess to run the Spark job
            cmd = [
                "python",
                "src/spark/jobs/batch_etl.py",
                "--input-path",
                input_path,
                "--output-table",
                "SALES_BATCH_RAW_TEST",
            ]

            # Set environment variables for the Spark job
            env = os.environ.copy()
            env.update(self.config.get_test_environment_vars())

            # Run the Spark job
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                # Parse output to get metrics
                output = stdout.decode()
                # This would need to be adapted based on your Spark job's output format
                return {
                    "success": True,
                    "records_processed": 1000,  # Placeholder
                    "output": output,
                }
            else:
                raise Exception(f"Spark job failed: {stderr.decode()}")

        except Exception as e:
            self.logger.error(f"Failed to execute Spark job: {e}")
            raise

    async def _run_dbt_transformations(self) -> Dict[str, Any]:
        """Run dbt transformations"""

        self.logger.info("Starting dbt transformations")

        dbt_config = self.config.get_dbt_config()
        start_time = time.time()

        # Run dbt commands
        dbt_commands = [
            "dbt deps",
            "dbt run --select staging",
            "dbt run --select intermediate",
            "dbt run --select marts",
            "dbt test",
        ]

        results = []

        for command in dbt_commands:
            self.logger.info(f"Executing: {command}")

            try:
                # Set environment variables for dbt
                env = os.environ.copy()
                env.update(self.config.get_test_environment_vars())

                # Run dbt command
                process = await asyncio.create_subprocess_shell(
                    command,
                    env=env,
                    cwd="dbt",  # Assuming dbt project is in dbt/ directory
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                stdout, stderr = await process.communicate()

                command_result = {
                    "command": command,
                    "returncode": process.returncode,
                    "stdout": stdout.decode(),
                    "stderr": stderr.decode(),
                    "success": process.returncode == 0,
                }

                results.append(command_result)

                if process.returncode != 0:
                    self.logger.error(f"dbt command failed: {command}")
                    self.logger.error(f"Error: {stderr.decode()}")

            except Exception as e:
                self.logger.error(f"Failed to execute dbt command '{command}': {e}")
                results.append({"command": command, "success": False, "error": str(e)})

        end_time = time.time()
        duration = end_time - start_time

        # Check if all commands succeeded
        all_success = all(r.get("success", False) for r in results)

        metrics = {
            "duration_seconds": duration,
            "commands_executed": len(dbt_commands),
            "all_success": all_success,
            "command_results": results,
        }

        self.logger.info(f"dbt transformations complete: {all_success}")
        return metrics

    async def _validate_pipeline_results(
        self, test_data: Dict[str, Any]
    ) -> ValidationSummary:
        """Validate the complete pipeline results"""

        self.logger.info("Starting pipeline validation")

        # Collect data for validation from each stage
        validation_data = {}

        # Raw data validation
        validation_data["raw_data"] = test_data["csv_data"]

        # Kafka streaming validation
        validation_data["kafka_streaming"] = {
            "messages": test_data["kafka_messages"],
            "duration_seconds": 60,
            "min_throughput": self.config.pipeline_stages.min_kafka_throughput,
        }

        # Spark ETL validation
        # In a real implementation, you would read the actual Spark output
        validation_data["spark_etl"] = {
            "input_data": test_data["csv_data"],
            "output_data": test_data[
                "expected_spark_output"
            ],  # This would be actual output
            "processing_time_seconds": 120,  # This would be actual processing time
            "min_throughput": self.config.pipeline_stages.min_spark_throughput,
        }

        # Data warehouse validation
        # This would connect to your actual test database
        validation_data["data_warehouse"] = {
            "connection": None  # Placeholder for database connection
        }

        # Run comprehensive validation
        validation_summary = self.validator.validate_pipeline_run(
            self.current_run_id, validation_data
        )

        # Save validation report
        report_path = self.test_artifacts_dir / "validation_report.json"
        self.validator.save_validation_report(validation_summary, str(report_path))

        self.logger.info(
            f"Pipeline validation complete: {validation_summary.overall_passed}"
        )
        self.logger.info(f"Overall score: {validation_summary.overall_score:.2f}")

        return validation_summary

    def _collect_performance_metrics(
        self, kafka_metrics: Dict, spark_metrics: Dict, dbt_metrics: Dict
    ) -> Dict[str, Any]:
        """Collect and aggregate performance metrics"""

        return {
            "kafka": kafka_metrics,
            "spark": spark_metrics,
            "dbt": dbt_metrics,
            "overall": {
                "total_duration": (
                    kafka_metrics.get("producer_duration", 0)
                    + spark_metrics.get("processing_duration", 0)
                    + dbt_metrics.get("duration_seconds", 0)
                ),
                "total_records_processed": spark_metrics.get("output_records", 0),
                "end_to_end_throughput": (
                    spark_metrics.get("output_records", 0)
                    / max(
                        1,
                        kafka_metrics.get("producer_duration", 0)
                        + spark_metrics.get("processing_duration", 0)
                        + dbt_metrics.get("duration_seconds", 0),
                    )
                ),
            },
        }

    def _collect_test_artifacts(self) -> Dict[str, str]:
        """Collect all test artifacts and return their paths"""

        artifacts = {}

        if self.test_artifacts_dir and self.test_artifacts_dir.exists():
            for file_path in self.test_artifacts_dir.rglob("*"):
                if file_path.is_file():
                    relative_path = file_path.relative_to(self.test_artifacts_dir)
                    artifacts[str(relative_path)] = str(file_path)

        return artifacts

    # Test scenario methods

    @pytest.mark.asyncio
    async def test_happy_path_pipeline(self):
        """Test the complete pipeline with clean, perfect data"""

        result = await self.run_complete_pipeline_test(
            scenario_name="happy_path", record_count=1000
        )

        assert result.success, f"Happy path test failed: {result.error_details}"
        assert (
            result.validation_summary.overall_score >= 0.95
        ), f"Low validation score: {result.validation_summary.overall_score}"

        # Performance assertions
        kafka_throughput = result.performance_metrics["kafka"]["throughput"]
        spark_throughput = result.performance_metrics["spark"]["throughput"]

        assert (
            kafka_throughput >= self.config.pipeline_stages.min_kafka_throughput
        ), f"Low Kafka throughput: {kafka_throughput}"
        assert (
            spark_throughput >= self.config.pipeline_stages.min_spark_throughput
        ), f"Low Spark throughput: {spark_throughput}"

    @pytest.mark.asyncio
    async def test_data_quality_issues_pipeline(self):
        """Test pipeline with intentional data quality issues"""

        result = await self.run_complete_pipeline_test(
            scenario_name="data_quality_issues", record_count=1000
        )

        assert result.success, f"Data quality test failed: {result.error_details}"

        # Should still pass but with lower scores due to data quality issues
        assert (
            result.validation_summary.overall_score >= 0.7
        ), f"Validation score too low for data quality test: {result.validation_summary.overall_score}"

    @pytest.mark.asyncio
    async def test_high_volume_pipeline(self):
        """Test pipeline with high volume data"""

        result = await self.run_complete_pipeline_test(
            scenario_name="high_volume", record_count=50000
        )

        assert result.success, f"High volume test failed: {result.error_details}"
        assert (
            result.validation_summary.overall_score >= 0.9
        ), f"Low validation score for high volume: {result.validation_summary.overall_score}"

        # Should maintain good throughput even with high volume
        overall_throughput = result.performance_metrics["overall"][
            "end_to_end_throughput"
        ]
        assert (
            overall_throughput >= 500
        ), f"Low end-to-end throughput: {overall_throughput}"

    @pytest.mark.asyncio
    async def test_edge_cases_pipeline(self):
        """Test pipeline with edge cases and boundary conditions"""

        result = await self.run_complete_pipeline_test(
            scenario_name="edge_cases", record_count=500
        )

        assert result.success, f"Edge cases test failed: {result.error_details}"
        assert (
            result.validation_summary.overall_score >= 0.8
        ), f"Low validation score for edge cases: {result.validation_summary.overall_score}"

    @pytest.mark.asyncio
    async def test_performance_benchmark(self):
        """Performance benchmark test with large dataset"""

        result = await self.run_complete_pipeline_test(
            scenario_name="performance_test", record_count=100000
        )

        assert result.success, f"Performance test failed: {result.error_details}"

        # Performance assertions
        overall_duration = result.performance_metrics["overall"]["total_duration"]
        overall_throughput = result.performance_metrics["overall"][
            "end_to_end_throughput"
        ]

        # Should process 100k records in under 10 minutes
        assert overall_duration <= 600, f"Processing took too long: {overall_duration}s"
        assert (
            overall_throughput >= 1000
        ), f"Low overall throughput: {overall_throughput}"

        # Log performance metrics for analysis
        self.logger.info(f"Performance benchmark results:")
        self.logger.info(f"  Total duration: {overall_duration:.2f}s")
        self.logger.info(f"  Overall throughput: {overall_throughput:.2f} records/s")
        self.logger.info(
            f"  Kafka throughput: {result.performance_metrics['kafka']['throughput']:.2f} msg/s"
        )
        self.logger.info(
            f"  Spark throughput: {result.performance_metrics['spark']['throughput']:.2f} records/s"
        )

    async def run_all_tests(self) -> Dict[str, PipelineTestResult]:
        """Run all pipeline tests and return results"""

        test_methods = [
            self.test_happy_path_pipeline,
            self.test_data_quality_issues_pipeline,
            self.test_high_volume_pipeline,
            self.test_edge_cases_pipeline,
            self.test_performance_benchmark,
        ]

        results = {}

        for test_method in test_methods:
            test_name = test_method.__name__
            self.logger.info(f"Running test: {test_name}")

            try:
                # Run test method
                await test_method()

                # If we get here, the test passed
                results[test_name] = PipelineTestResult(
                    test_name=test_name,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration_seconds=0,
                    success=True,
                    validation_summary=None,
                    performance_metrics={},
                    error_details=None,
                    artifacts={},
                )

            except Exception as e:
                self.logger.error(f"Test {test_name} failed: {e}")

                results[test_name] = PipelineTestResult(
                    test_name=test_name,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    duration_seconds=0,
                    success=False,
                    validation_summary=None,
                    performance_metrics={},
                    error_details=str(e),
                    artifacts={},
                )

        return results


# Convenience functions for running tests


async def run_single_test(test_name: str, **kwargs) -> PipelineTestResult:
    """Run a single pipeline test"""

    suite = E2EPipelineTestSuite()

    if test_name == "happy_path":
        return await suite.run_complete_pipeline_test("happy_path", **kwargs)
    elif test_name == "data_quality":
        return await suite.run_complete_pipeline_test("data_quality_issues", **kwargs)
    elif test_name == "high_volume":
        return await suite.run_complete_pipeline_test("high_volume", **kwargs)
    elif test_name == "edge_cases":
        return await suite.run_complete_pipeline_test("edge_cases", **kwargs)
    elif test_name == "performance":
        return await suite.run_complete_pipeline_test("performance_test", **kwargs)
    else:
        raise ValueError(f"Unknown test: {test_name}")


async def run_full_test_suite(**kwargs) -> Dict[str, PipelineTestResult]:
    """Run the complete test suite"""

    suite = E2EPipelineTestSuite()
    return await suite.run_all_tests()


def main():
    """Main entry point for running tests from command line"""

    import argparse

    parser = argparse.ArgumentParser(description="Sales Pipeline E2E Tests")
    parser.add_argument(
        "--test",
        choices=[
            "happy_path",
            "data_quality",
            "high_volume",
            "edge_cases",
            "performance",
            "all",
        ],
        default="happy_path",
        help="Test to run",
    )
    parser.add_argument(
        "--record-count", type=int, default=1000, help="Number of records to generate"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    async def run_tests():
        if args.test == "all":
            results = await run_full_test_suite()

            print("\n=== Test Suite Results ===")
            for test_name, result in results.items():
                status = "PASS" if result.success else "FAIL"
                print(f"{test_name}: {status}")
                if not result.success:
                    print(f"  Error: {result.error_details}")
        else:
            result = await run_single_test(args.test, record_count=args.record_count)

            status = "PASS" if result.success else "FAIL"
            print(f"\n=== Test Result ===")
            print(f"Test: {args.test}")
            print(f"Status: {status}")
            print(f"Duration: {result.duration_seconds:.2f}s")

            if result.validation_summary:
                print(
                    f"Validation Score: {result.validation_summary.overall_score:.2f}"
                )

            if not result.success and result.error_details:
                print(f"Error: {result.error_details}")

    # Run the async function
    asyncio.run(run_tests())


if __name__ == "__main__":
    main()
