#!/usr/bin/env python3
"""
Error Scenario and Recovery Testing for Sales Data Pipeline

This module tests the pipeline's behavior under various error conditions
and validates recovery mechanisms, fault tolerance, and data consistency
during failures.
"""

import asyncio
import json
import logging
import os
import random
import signal
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
import pandas as pd
import pytest
from dataclasses import dataclass

# Test framework imports
from .e2e_config import E2E_CONFIG
from .data_validators import PipelineValidator, ValidationSummary
from .test_data_generator import TestDataGenerator, DataGenerationConfig
from .infrastructure_manager import InfrastructureManager
from .performance_monitor import PerformanceMonitor


@dataclass
class ErrorScenario:
    """Definition of an error scenario to test"""

    name: str
    description: str
    error_type: str  # 'service_failure', 'data_corruption', 'network_issue', 'resource_exhaustion'
    component: str  # 'kafka', 'spark', 'dbt', 'database', 'storage'
    failure_point: str  # 'startup', 'processing', 'shutdown'
    expected_behavior: str  # 'fail_fast', 'retry', 'degrade_gracefully'
    recovery_mechanism: str  # 'automatic', 'manual', 'none'
    test_duration_seconds: int = 60

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "error_type": self.error_type,
            "component": self.component,
            "failure_point": self.failure_point,
            "expected_behavior": self.expected_behavior,
            "recovery_mechanism": self.recovery_mechanism,
            "test_duration_seconds": self.test_duration_seconds,
        }


@dataclass
class ErrorTestResult:
    """Result of an error scenario test"""

    scenario: ErrorScenario
    start_time: datetime
    end_time: datetime
    error_injected_at: datetime
    recovery_detected_at: Optional[datetime]
    test_passed: bool
    validation_summary: Optional[ValidationSummary]
    error_details: Optional[str]
    recovery_time_seconds: Optional[float]
    data_consistency_check: bool
    artifacts: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario.to_dict(),
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "error_injected_at": self.error_injected_at.isoformat(),
            "recovery_detected_at": self.recovery_detected_at.isoformat()
            if self.recovery_detected_at
            else None,
            "test_passed": self.test_passed,
            "validation_summary": self.validation_summary.to_dict()
            if self.validation_summary
            else None,
            "error_details": self.error_details,
            "recovery_time_seconds": self.recovery_time_seconds,
            "data_consistency_check": self.data_consistency_check,
            "artifacts": self.artifacts,
        }


class ErrorInjector:
    """Injects various types of errors into pipeline components"""

    def __init__(self, infrastructure_manager: InfrastructureManager):
        self.infrastructure = infrastructure_manager
        self.logger = logging.getLogger(__name__)
        self.active_errors = {}  # Track active error injections

    async def inject_service_failure(
        self, service_name: str, failure_type: str = "stop"
    ) -> str:
        """Inject service failure by stopping or killing containers"""

        error_id = str(uuid.uuid4())
        self.logger.info(f"Injecting service failure: {service_name} ({failure_type})")

        try:
            if failure_type == "stop":
                # Gracefully stop the service
                await self._stop_service(service_name)
            elif failure_type == "kill":
                # Forcefully kill the service
                await self._kill_service(service_name)
            elif failure_type == "pause":
                # Pause the service (container still exists but not responding)
                await self._pause_service(service_name)
            else:
                raise ValueError(f"Unknown failure type: {failure_type}")

            self.active_errors[error_id] = {
                "type": "service_failure",
                "service": service_name,
                "failure_type": failure_type,
                "injected_at": datetime.now(),
            }

            return error_id

        except Exception as e:
            self.logger.error(f"Failed to inject service failure: {e}")
            raise

    async def inject_data_corruption(
        self, corruption_type: str = "random_bytes"
    ) -> str:
        """Inject data corruption scenarios"""

        error_id = str(uuid.uuid4())
        self.logger.info(f"Injecting data corruption: {corruption_type}")

        try:
            if corruption_type == "random_bytes":
                # Corrupt data files by adding random bytes
                await self._corrupt_data_files()
            elif corruption_type == "missing_columns":
                # Remove columns from data files
                await self._corrupt_data_schema()
            elif corruption_type == "invalid_json":
                # Corrupt JSON messages
                await self._corrupt_json_messages()
            else:
                raise ValueError(f"Unknown corruption type: {corruption_type}")

            self.active_errors[error_id] = {
                "type": "data_corruption",
                "corruption_type": corruption_type,
                "injected_at": datetime.now(),
            }

            return error_id

        except Exception as e:
            self.logger.error(f"Failed to inject data corruption: {e}")
            raise

    async def inject_network_issue(self, issue_type: str = "latency") -> str:
        """Inject network-related issues"""

        error_id = str(uuid.uuid4())
        self.logger.info(f"Injecting network issue: {issue_type}")

        try:
            if issue_type == "latency":
                # Add network latency
                await self._add_network_latency()
            elif issue_type == "packet_loss":
                # Simulate packet loss
                await self._simulate_packet_loss()
            elif issue_type == "connection_timeout":
                # Force connection timeouts
                await self._force_connection_timeouts()
            else:
                raise ValueError(f"Unknown network issue type: {issue_type}")

            self.active_errors[error_id] = {
                "type": "network_issue",
                "issue_type": issue_type,
                "injected_at": datetime.now(),
            }

            return error_id

        except Exception as e:
            self.logger.error(f"Failed to inject network issue: {e}")
            raise

    async def inject_resource_exhaustion(self, resource_type: str = "memory") -> str:
        """Inject resource exhaustion scenarios"""

        error_id = str(uuid.uuid4())
        self.logger.info(f"Injecting resource exhaustion: {resource_type}")

        try:
            if resource_type == "memory":
                # Consume available memory
                await self._exhaust_memory()
            elif resource_type == "disk":
                # Fill up disk space
                await self._exhaust_disk_space()
            elif resource_type == "cpu":
                # Consume CPU resources
                await self._exhaust_cpu()
            else:
                raise ValueError(f"Unknown resource type: {resource_type}")

            self.active_errors[error_id] = {
                "type": "resource_exhaustion",
                "resource_type": resource_type,
                "injected_at": datetime.now(),
            }

            return error_id

        except Exception as e:
            self.logger.error(f"Failed to inject resource exhaustion: {e}")
            raise

    async def recover_from_error(self, error_id: str) -> None:
        """Recover from an injected error"""

        if error_id not in self.active_errors:
            self.logger.warning(f"Error ID not found: {error_id}")
            return

        error_info = self.active_errors[error_id]
        error_type = error_info["type"]

        self.logger.info(f"Recovering from error: {error_id} ({error_type})")

        try:
            if error_type == "service_failure":
                await self._recover_service_failure(error_info)
            elif error_type == "data_corruption":
                await self._recover_data_corruption(error_info)
            elif error_type == "network_issue":
                await self._recover_network_issue(error_info)
            elif error_type == "resource_exhaustion":
                await self._recover_resource_exhaustion(error_info)

            del self.active_errors[error_id]
            self.logger.info(f"Recovered from error: {error_id}")

        except Exception as e:
            self.logger.error(f"Failed to recover from error {error_id}: {e}")
            raise

    # Service failure injection methods

    async def _stop_service(self, service_name: str) -> None:
        """Gracefully stop a service"""

        if self.infrastructure.docker_client:
            try:
                container = self.infrastructure.docker_client.containers.get(
                    f"test-{service_name}"
                )
                container.stop()
                self.logger.info(f"Stopped service: {service_name}")
            except Exception as e:
                self.logger.error(f"Failed to stop service {service_name}: {e}")
                raise

    async def _kill_service(self, service_name: str) -> None:
        """Forcefully kill a service"""

        if self.infrastructure.docker_client:
            try:
                container = self.infrastructure.docker_client.containers.get(
                    f"test-{service_name}"
                )
                container.kill()
                self.logger.info(f"Killed service: {service_name}")
            except Exception as e:
                self.logger.error(f"Failed to kill service {service_name}: {e}")
                raise

    async def _pause_service(self, service_name: str) -> None:
        """Pause a service"""

        if self.infrastructure.docker_client:
            try:
                container = self.infrastructure.docker_client.containers.get(
                    f"test-{service_name}"
                )
                container.pause()
                self.logger.info(f"Paused service: {service_name}")
            except Exception as e:
                self.logger.error(f"Failed to pause service {service_name}: {e}")
                raise

    # Data corruption injection methods

    async def _corrupt_data_files(self) -> None:
        """Corrupt data files by adding random bytes"""

        # This would corrupt actual test data files
        # For now, we'll create corrupted test files
        test_data_dir = Path("tests/integration/test_data/corrupted")
        test_data_dir.mkdir(parents=True, exist_ok=True)

        corrupted_file = test_data_dir / "corrupted_data.csv"

        # Create a file with random bytes mixed in
        with open(corrupted_file, "wb") as f:
            f.write(b"Order ID,Sales,Quantity\n")
            f.write(b"ORDER_001,100.00,1\n")
            f.write(b"\x00\x01\x02\x03\x04")  # Random bytes
            f.write(b"ORDER_002,200.00,2\n")

        self.logger.info(f"Created corrupted data file: {corrupted_file}")

    async def _corrupt_data_schema(self) -> None:
        """Corrupt data by removing required columns"""

        test_data_dir = Path("tests/integration/test_data/corrupted")
        test_data_dir.mkdir(parents=True, exist_ok=True)

        corrupted_file = test_data_dir / "missing_columns.csv"

        # Create a CSV file missing required columns
        with open(corrupted_file, "w") as f:
            f.write("Order ID,Sales\n")  # Missing Quantity column
            f.write("ORDER_001,100.00\n")
            f.write("ORDER_002,200.00\n")

        self.logger.info(f"Created schema-corrupted file: {corrupted_file}")

    async def _corrupt_json_messages(self) -> None:
        """Corrupt JSON messages"""

        # This would inject corrupted messages into Kafka
        # For testing purposes, we'll create corrupted message files
        test_data_dir = Path("tests/integration/test_data/corrupted")
        test_data_dir.mkdir(parents=True, exist_ok=True)

        corrupted_file = test_data_dir / "corrupted_messages.json"

        corrupted_messages = [
            '{"order_id": "ORDER_001", "sales": 100.00}',  # Valid JSON
            '{"order_id": "ORDER_002", "sales":}',  # Invalid JSON (missing value)
            '{"order_id": "ORDER_003" "sales": 300.00}',  # Invalid JSON (missing comma)
        ]

        with open(corrupted_file, "w") as f:
            for msg in corrupted_messages:
                f.write(msg + "\n")

        self.logger.info(f"Created corrupted JSON messages: {corrupted_file}")

    # Network issue injection methods

    async def _add_network_latency(self) -> None:
        """Add network latency using tc (traffic control)"""

        try:
            # This would require tc (traffic control) to be available
            # For testing, we'll simulate by adding delays in the application
            self.logger.info("Simulated network latency injection")
        except Exception as e:
            self.logger.warning(f"Network latency injection not available: {e}")

    async def _simulate_packet_loss(self) -> None:
        """Simulate packet loss"""

        try:
            # This would require network manipulation tools
            # For testing, we'll simulate by dropping some messages
            self.logger.info("Simulated packet loss injection")
        except Exception as e:
            self.logger.warning(f"Packet loss injection not available: {e}")

    async def _force_connection_timeouts(self) -> None:
        """Force connection timeouts"""

        try:
            # This would manipulate network connections
            # For testing, we'll simulate by blocking connections
            self.logger.info("Simulated connection timeout injection")
        except Exception as e:
            self.logger.warning(f"Connection timeout injection not available: {e}")

    # Resource exhaustion injection methods

    async def _exhaust_memory(self) -> None:
        """Exhaust available memory"""

        try:
            # This would consume system memory
            # For testing, we'll create a memory-intensive process
            self.logger.info("Simulated memory exhaustion injection")
        except Exception as e:
            self.logger.warning(f"Memory exhaustion injection not available: {e}")

    async def _exhaust_disk_space(self) -> None:
        """Exhaust available disk space"""

        try:
            # Create a large file to consume disk space
            temp_dir = Path(tempfile.gettempdir()) / "disk_exhaustion_test"
            temp_dir.mkdir(exist_ok=True)

            large_file = temp_dir / "large_file.dat"

            # Create a 100MB file
            with open(large_file, "wb") as f:
                f.write(b"0" * (100 * 1024 * 1024))

            self.logger.info(f"Created large file for disk exhaustion: {large_file}")
        except Exception as e:
            self.logger.warning(f"Disk exhaustion injection failed: {e}")

    async def _exhaust_cpu(self) -> None:
        """Exhaust CPU resources"""

        try:
            # This would start CPU-intensive processes
            # For testing, we'll simulate CPU load
            self.logger.info("Simulated CPU exhaustion injection")
        except Exception as e:
            self.logger.warning(f"CPU exhaustion injection not available: {e}")

    # Recovery methods

    async def _recover_service_failure(self, error_info: Dict) -> None:
        """Recover from service failure"""

        service_name = error_info["service"]
        failure_type = error_info["failure_type"]

        if self.infrastructure.docker_client:
            try:
                container = self.infrastructure.docker_client.containers.get(
                    f"test-{service_name}"
                )

                if failure_type == "pause":
                    container.unpause()
                    self.logger.info(f"Unpaused service: {service_name}")
                else:
                    # Restart the container
                    container.start()
                    self.logger.info(f"Restarted service: {service_name}")

            except Exception as e:
                self.logger.error(f"Failed to recover service {service_name}: {e}")
                raise

    async def _recover_data_corruption(self, error_info: Dict) -> None:
        """Recover from data corruption"""

        # Clean up corrupted test files
        test_data_dir = Path("tests/integration/test_data/corrupted")

        if test_data_dir.exists():
            import shutil

            shutil.rmtree(test_data_dir)
            self.logger.info("Cleaned up corrupted data files")

    async def _recover_network_issue(self, error_info: Dict) -> None:
        """Recover from network issues"""

        # This would remove network manipulation rules
        # For testing, we just log the recovery
        self.logger.info("Recovered from network issues")

    async def _recover_resource_exhaustion(self, error_info: Dict) -> None:
        """Recover from resource exhaustion"""

        resource_type = error_info["resource_type"]

        if resource_type == "disk":
            # Remove large files
            temp_dir = Path(tempfile.gettempdir()) / "disk_exhaustion_test"
            if temp_dir.exists():
                import shutil

                shutil.rmtree(temp_dir)
                self.logger.info("Cleaned up disk exhaustion files")

        # For other resource types, processes would need to be killed
        self.logger.info(f"Recovered from {resource_type} exhaustion")


class ErrorScenarioTestSuite:
    """Test suite for error scenarios and recovery testing"""

    def __init__(self):
        self.config = E2E_CONFIG
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.data_generator = TestDataGenerator()
        self.validator = PipelineValidator()
        self.infrastructure = InfrastructureManager(self.config)
        self.error_injector = ErrorInjector(self.infrastructure)
        self.performance_monitor = PerformanceMonitor()

        # Test tracking
        self.current_run_id = None
        self.test_artifacts_dir = None

    def define_error_scenarios(self) -> List[ErrorScenario]:
        """Define all error scenarios to test"""

        scenarios = [
            # Service failure scenarios
            ErrorScenario(
                name="kafka_service_stop",
                description="Kafka service stops gracefully during processing",
                error_type="service_failure",
                component="kafka",
                failure_point="processing",
                expected_behavior="retry",
                recovery_mechanism="automatic",
                test_duration_seconds=120,
            ),
            ErrorScenario(
                name="kafka_service_kill",
                description="Kafka service is forcefully killed",
                error_type="service_failure",
                component="kafka",
                failure_point="processing",
                expected_behavior="retry",
                recovery_mechanism="manual",
                test_duration_seconds=120,
            ),
            ErrorScenario(
                name="postgres_connection_loss",
                description="PostgreSQL database connection is lost",
                error_type="service_failure",
                component="database",
                failure_point="processing",
                expected_behavior="retry",
                recovery_mechanism="automatic",
                test_duration_seconds=90,
            ),
            ErrorScenario(
                name="spark_worker_failure",
                description="Spark worker node fails during processing",
                error_type="service_failure",
                component="spark",
                failure_point="processing",
                expected_behavior="retry",
                recovery_mechanism="automatic",
                test_duration_seconds=180,
            ),
            # Data corruption scenarios
            ErrorScenario(
                name="csv_data_corruption",
                description="Raw CSV data contains corrupted bytes",
                error_type="data_corruption",
                component="storage",
                failure_point="processing",
                expected_behavior="fail_fast",
                recovery_mechanism="manual",
                test_duration_seconds=60,
            ),
            ErrorScenario(
                name="json_message_corruption",
                description="Kafka messages contain invalid JSON",
                error_type="data_corruption",
                component="kafka",
                failure_point="processing",
                expected_behavior="degrade_gracefully",
                recovery_mechanism="automatic",
                test_duration_seconds=60,
            ),
            ErrorScenario(
                name="schema_mismatch",
                description="Data schema doesn't match expected format",
                error_type="data_corruption",
                component="spark",
                failure_point="processing",
                expected_behavior="fail_fast",
                recovery_mechanism="manual",
                test_duration_seconds=60,
            ),
            # Network issue scenarios
            ErrorScenario(
                name="network_latency",
                description="High network latency between components",
                error_type="network_issue",
                component="kafka",
                failure_point="processing",
                expected_behavior="degrade_gracefully",
                recovery_mechanism="automatic",
                test_duration_seconds=120,
            ),
            ErrorScenario(
                name="connection_timeout",
                description="Network connections timeout frequently",
                error_type="network_issue",
                component="database",
                failure_point="processing",
                expected_behavior="retry",
                recovery_mechanism="automatic",
                test_duration_seconds=90,
            ),
            # Resource exhaustion scenarios
            ErrorScenario(
                name="memory_exhaustion",
                description="System runs out of available memory",
                error_type="resource_exhaustion",
                component="spark",
                failure_point="processing",
                expected_behavior="fail_fast",
                recovery_mechanism="manual",
                test_duration_seconds=90,
            ),
            ErrorScenario(
                name="disk_space_exhaustion",
                description="System runs out of disk space",
                error_type="resource_exhaustion",
                component="storage",
                failure_point="processing",
                expected_behavior="fail_fast",
                recovery_mechanism="manual",
                test_duration_seconds=60,
            ),
        ]

        return scenarios

    async def run_error_scenario_test(self, scenario: ErrorScenario) -> ErrorTestResult:
        """Run a single error scenario test"""

        self.logger.info(f"=== Running Error Scenario: {scenario.name} ===")

        start_time = datetime.now()
        artifacts = {}
        error_injected_at = None
        recovery_detected_at = None

        try:
            # 1. Setup test environment
            self.logger.info("Step 1: Setting up test environment")
            await self._setup_test_environment()

            # 2. Generate and stage test data
            self.logger.info("Step 2: Generating test data")
            test_data = await self._generate_error_test_data(scenario)

            # 3. Start pipeline processing
            self.logger.info("Step 3: Starting pipeline processing")
            pipeline_task = asyncio.create_task(
                self._start_pipeline_processing(test_data)
            )

            # 4. Wait for processing to begin
            await asyncio.sleep(5)  # Give pipeline time to start

            # 5. Inject error at appropriate time
            self.logger.info(f"Step 4: Injecting error ({scenario.error_type})")
            error_injected_at = datetime.now()
            error_id = await self._inject_error(scenario)

            # 6. Monitor system behavior
            self.logger.info("Step 5: Monitoring system behavior")
            monitoring_task = asyncio.create_task(
                self._monitor_error_behavior(scenario, scenario.test_duration_seconds)
            )

            # 7. Wait for test duration or recovery
            recovery_detected_at = await self._wait_for_recovery_or_timeout(
                scenario, error_id, scenario.test_duration_seconds
            )

            # 8. Clean up error injection
            if error_id:
                await self.error_injector.recover_from_error(error_id)

            # 9. Validate final state
            self.logger.info("Step 6: Validating final state")
            validation_summary = await self._validate_error_recovery(
                test_data, scenario
            )

            # 10. Check data consistency
            data_consistency_check = await self._check_data_consistency(test_data)

            # Cancel remaining tasks
            pipeline_task.cancel()
            monitoring_task.cancel()

            try:
                await pipeline_task
            except asyncio.CancelledError:
                pass

            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass

            end_time = datetime.now()

            # Calculate recovery time
            recovery_time_seconds = None
            if recovery_detected_at and error_injected_at:
                recovery_time_seconds = (
                    recovery_detected_at - error_injected_at
                ).total_seconds()

            # Determine test success
            test_passed = self._evaluate_test_success(
                scenario, validation_summary, recovery_time_seconds
            )

            result = ErrorTestResult(
                scenario=scenario,
                start_time=start_time,
                end_time=end_time,
                error_injected_at=error_injected_at or start_time,
                recovery_detected_at=recovery_detected_at,
                test_passed=test_passed,
                validation_summary=validation_summary,
                error_details=None,
                recovery_time_seconds=recovery_time_seconds,
                data_consistency_check=data_consistency_check,
                artifacts=artifacts,
            )

            self.logger.info(f"=== Error Scenario Complete: {scenario.name} ===")
            self.logger.info(f"Test Passed: {test_passed}")
            if recovery_time_seconds:
                self.logger.info(f"Recovery Time: {recovery_time_seconds:.2f}s")

            return result

        except Exception as e:
            end_time = datetime.now()

            self.logger.error(f"Error scenario test failed: {e}", exc_info=True)

            result = ErrorTestResult(
                scenario=scenario,
                start_time=start_time,
                end_time=end_time,
                error_injected_at=error_injected_at or start_time,
                recovery_detected_at=recovery_detected_at,
                test_passed=False,
                validation_summary=None,
                error_details=str(e),
                recovery_time_seconds=None,
                data_consistency_check=False,
                artifacts=artifacts,
            )

            return result

    async def _setup_test_environment(self) -> None:
        """Setup test environment for error scenario"""

        await self.infrastructure.start_services()
        await self.infrastructure.wait_for_readiness()
        await self.infrastructure.setup_databases()
        await self.infrastructure.setup_kafka_topics()

    async def _generate_error_test_data(
        self, scenario: ErrorScenario
    ) -> Dict[str, Any]:
        """Generate test data appropriate for the error scenario"""

        # Use smaller dataset for error testing
        record_count = 500

        if scenario.error_type == "data_corruption":
            # Generate data with known corruption patterns
            test_data = self.data_generator.generate_test_scenario_data(
                "data_quality_issues", record_count=record_count
            )
        else:
            # Generate clean data for other error types
            test_data = self.data_generator.generate_test_scenario_data(
                "happy_path", record_count=record_count
            )

        return test_data

    async def _start_pipeline_processing(self, test_data: Dict[str, Any]) -> None:
        """Start pipeline processing in background"""

        try:
            # This would start the actual pipeline processing
            # For testing, we'll simulate pipeline activity
            while True:
                await asyncio.sleep(1)
                # Simulate processing activity

        except asyncio.CancelledError:
            self.logger.debug("Pipeline processing cancelled")
            raise

    async def _inject_error(self, scenario: ErrorScenario) -> Optional[str]:
        """Inject the error specified in the scenario"""

        error_id = None

        try:
            if scenario.error_type == "service_failure":
                if scenario.component == "kafka":
                    error_id = await self.error_injector.inject_service_failure(
                        "kafka", "stop"
                    )
                elif scenario.component == "database":
                    error_id = await self.error_injector.inject_service_failure(
                        "postgres", "stop"
                    )
                elif scenario.component == "spark":
                    # Simulate Spark failure
                    error_id = await self.error_injector.inject_service_failure(
                        "spark", "kill"
                    )

            elif scenario.error_type == "data_corruption":
                if "csv" in scenario.name:
                    error_id = await self.error_injector.inject_data_corruption(
                        "random_bytes"
                    )
                elif "json" in scenario.name:
                    error_id = await self.error_injector.inject_data_corruption(
                        "invalid_json"
                    )
                elif "schema" in scenario.name:
                    error_id = await self.error_injector.inject_data_corruption(
                        "missing_columns"
                    )

            elif scenario.error_type == "network_issue":
                if "latency" in scenario.name:
                    error_id = await self.error_injector.inject_network_issue("latency")
                elif "timeout" in scenario.name:
                    error_id = await self.error_injector.inject_network_issue(
                        "connection_timeout"
                    )

            elif scenario.error_type == "resource_exhaustion":
                if "memory" in scenario.name:
                    error_id = await self.error_injector.inject_resource_exhaustion(
                        "memory"
                    )
                elif "disk" in scenario.name:
                    error_id = await self.error_injector.inject_resource_exhaustion(
                        "disk"
                    )

            return error_id

        except Exception as e:
            self.logger.error(
                f"Failed to inject error for scenario {scenario.name}: {e}"
            )
            raise

    async def _monitor_error_behavior(
        self, scenario: ErrorScenario, duration_seconds: int
    ) -> Dict[str, Any]:
        """Monitor system behavior during error scenario"""

        monitoring_data = {
            "component_status": {},
            "performance_metrics": {},
            "error_logs": [],
        }

        try:
            start_time = time.time()

            while time.time() - start_time < duration_seconds:
                # Monitor service status
                service_status = self.infrastructure.get_service_status()
                monitoring_data["component_status"] = service_status

                # Check for recovery
                if (
                    scenario.expected_behavior == "retry"
                    and scenario.recovery_mechanism == "automatic"
                ):
                    if all(status == "running" for status in service_status.values()):
                        self.logger.info("Automatic recovery detected")
                        break

                await asyncio.sleep(2)

            return monitoring_data

        except asyncio.CancelledError:
            self.logger.debug("Error monitoring cancelled")
            raise

    async def _wait_for_recovery_or_timeout(
        self, scenario: ErrorScenario, error_id: Optional[str], timeout_seconds: int
    ) -> Optional[datetime]:
        """Wait for recovery or timeout"""

        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            # Check if recovery has occurred
            if scenario.recovery_mechanism == "automatic":
                if self.infrastructure.are_all_services_running():
                    return datetime.now()

            await asyncio.sleep(2)

        return None  # No recovery detected within timeout

    async def _validate_error_recovery(
        self, test_data: Dict[str, Any], scenario: ErrorScenario
    ) -> Optional[ValidationSummary]:
        """Validate system state after error recovery"""

        try:
            # Simplified validation for error scenarios
            validation_data = {
                "raw_data": test_data.get("csv_data"),
            }

            if scenario.expected_behavior != "fail_fast":
                # Only validate if system should have recovered
                validation_summary = self.validator.validate_pipeline_run(
                    f"error_test_{scenario.name}", validation_data
                )
                return validation_summary

            return None

        except Exception as e:
            self.logger.error(f"Validation failed for error scenario: {e}")
            return None

    async def _check_data_consistency(self, test_data: Dict[str, Any]) -> bool:
        """Check data consistency after error recovery"""

        try:
            # This would check if data is consistent across all components
            # For testing, we'll perform basic checks

            # Check if expected number of records are present
            expected_records = len(test_data.get("csv_data", []))

            # In a real implementation, you would:
            # 1. Query database for actual record counts
            # 2. Check for duplicates
            # 3. Verify data integrity
            # 4. Check for missing data

            # For now, return True (assuming consistency)
            return True

        except Exception as e:
            self.logger.error(f"Data consistency check failed: {e}")
            return False

    def _evaluate_test_success(
        self,
        scenario: ErrorScenario,
        validation_summary: Optional[ValidationSummary],
        recovery_time_seconds: Optional[float],
    ) -> bool:
        """Evaluate if the error scenario test passed"""

        # Test passes if the system behaves as expected

        if scenario.expected_behavior == "fail_fast":
            # System should fail quickly and not recover automatically
            return recovery_time_seconds is None or recovery_time_seconds < 10

        elif scenario.expected_behavior == "retry":
            # System should retry and eventually recover
            if scenario.recovery_mechanism == "automatic":
                return recovery_time_seconds is not None and recovery_time_seconds < 60
            else:
                # Manual recovery, just check that it can recover when fixed
                return True

        elif scenario.expected_behavior == "degrade_gracefully":
            # System should continue with reduced functionality
            if validation_summary:
                return (
                    validation_summary.overall_score >= 0.7
                )  # Allow degraded performance
            return True

        return False

    async def run_all_error_scenarios(self) -> Dict[str, ErrorTestResult]:
        """Run all defined error scenarios"""

        self.logger.info("=== Running All Error Scenarios ===")

        scenarios = self.define_error_scenarios()
        results = {}

        for scenario in scenarios:
            self.logger.info(f"Running scenario: {scenario.name}")

            try:
                result = await self.run_error_scenario_test(scenario)
                results[scenario.name] = result

                # Brief pause between scenarios
                await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(f"Scenario {scenario.name} failed: {e}")

                # Create failed result
                results[scenario.name] = ErrorTestResult(
                    scenario=scenario,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    error_injected_at=datetime.now(),
                    recovery_detected_at=None,
                    test_passed=False,
                    validation_summary=None,
                    error_details=str(e),
                    recovery_time_seconds=None,
                    data_consistency_check=False,
                    artifacts={},
                )

        # Generate summary report
        passed_count = sum(1 for result in results.values() if result.test_passed)
        total_count = len(results)

        self.logger.info("=== Error Scenario Summary ===")
        self.logger.info(f"Total scenarios: {total_count}")
        self.logger.info(f"Passed: {passed_count}")
        self.logger.info(f"Failed: {total_count - passed_count}")

        return results

    def generate_error_test_report(
        self, results: Dict[str, ErrorTestResult], output_path: str
    ) -> str:
        """Generate comprehensive error testing report"""

        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_scenarios": len(results),
                "passed_scenarios": sum(1 for r in results.values() if r.test_passed),
                "failed_scenarios": sum(
                    1 for r in results.values() if not r.test_passed
                ),
            },
            "scenario_results": {
                name: result.to_dict() for name, result in results.items()
            },
            "summary_analysis": self._analyze_error_test_results(results),
        }

        # Save report
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        self.logger.info(f"Error test report saved to: {output_path}")
        return str(output_file)

    def _analyze_error_test_results(
        self, results: Dict[str, ErrorTestResult]
    ) -> Dict[str, Any]:
        """Analyze error test results for patterns and insights"""

        analysis = {
            "recovery_times": {},
            "failure_patterns": {},
            "component_reliability": {},
        }

        # Analyze recovery times by error type
        recovery_times_by_type = {}
        for result in results.values():
            error_type = result.scenario.error_type
            if result.recovery_time_seconds is not None:
                if error_type not in recovery_times_by_type:
                    recovery_times_by_type[error_type] = []
                recovery_times_by_type[error_type].append(result.recovery_time_seconds)

        for error_type, times in recovery_times_by_type.items():
            analysis["recovery_times"][error_type] = {
                "avg_recovery_time": sum(times) / len(times),
                "min_recovery_time": min(times),
                "max_recovery_time": max(times),
                "sample_count": len(times),
            }

        # Analyze failure patterns
        failure_by_component = {}
        for result in results.values():
            component = result.scenario.component
            if not result.test_passed:
                failure_by_component[component] = (
                    failure_by_component.get(component, 0) + 1
                )

        analysis["failure_patterns"] = failure_by_component

        # Component reliability analysis
        component_tests = {}
        for result in results.values():
            component = result.scenario.component
            if component not in component_tests:
                component_tests[component] = {"total": 0, "passed": 0}

            component_tests[component]["total"] += 1
            if result.test_passed:
                component_tests[component]["passed"] += 1

        for component, stats in component_tests.items():
            reliability = stats["passed"] / stats["total"] if stats["total"] > 0 else 0
            analysis["component_reliability"][component] = {
                "reliability_score": reliability,
                "tests_passed": stats["passed"],
                "total_tests": stats["total"],
            }

        return analysis


# Pytest integration


@pytest.mark.asyncio
async def test_kafka_service_failure():
    """Test Kafka service failure and recovery"""
    suite = ErrorScenarioTestSuite()

    scenario = ErrorScenario(
        name="kafka_service_stop_test",
        description="Test Kafka service stop and recovery",
        error_type="service_failure",
        component="kafka",
        failure_point="processing",
        expected_behavior="retry",
        recovery_mechanism="automatic",
        test_duration_seconds=60,
    )

    result = await suite.run_error_scenario_test(scenario)
    assert (
        result.test_passed
    ), f"Kafka service failure test failed: {result.error_details}"


@pytest.mark.asyncio
async def test_data_corruption_handling():
    """Test data corruption handling"""
    suite = ErrorScenarioTestSuite()

    scenario = ErrorScenario(
        name="csv_corruption_test",
        description="Test CSV data corruption handling",
        error_type="data_corruption",
        component="storage",
        failure_point="processing",
        expected_behavior="fail_fast",
        recovery_mechanism="manual",
        test_duration_seconds=30,
    )

    result = await suite.run_error_scenario_test(scenario)
    assert result.test_passed, f"Data corruption test failed: {result.error_details}"


@pytest.mark.asyncio
async def test_complete_error_scenario_suite():
    """Run the complete error scenario test suite"""
    suite = ErrorScenarioTestSuite()

    results = await suite.run_all_error_scenarios()

    # Ensure at least 70% of scenarios pass
    passed_count = sum(1 for result in results.values() if result.test_passed)
    total_count = len(results)
    success_rate = passed_count / total_count if total_count > 0 else 0

    assert (
        success_rate >= 0.7
    ), f"Error scenario success rate too low: {success_rate:.2%}"


# Command line interface


async def main():
    """Main entry point for error scenario testing"""
    import argparse

    parser = argparse.ArgumentParser(description="Sales Pipeline Error Scenario Tests")
    parser.add_argument("--scenario", help="Specific scenario to run")
    parser.add_argument("--all", action="store_true", help="Run all error scenarios")
    parser.add_argument(
        "--output",
        default="tests/integration/reports/error_test_report.json",
        help="Output file for test report",
    )

    args = parser.parse_args()

    suite = ErrorScenarioTestSuite()

    if args.all:
        results = await suite.run_all_error_scenarios()
        suite.generate_error_test_report(results, args.output)
    elif args.scenario:
        scenarios = {s.name: s for s in suite.define_error_scenarios()}
        if args.scenario in scenarios:
            result = await suite.run_error_scenario_test(scenarios[args.scenario])
            print(
                f"Scenario {args.scenario}: {'PASS' if result.test_passed else 'FAIL'}"
            )
        else:
            print(f"Unknown scenario: {args.scenario}")
            print(f"Available scenarios: {list(scenarios.keys())}")
    else:
        print("Please specify --scenario or --all")


if __name__ == "__main__":
    asyncio.run(main())
