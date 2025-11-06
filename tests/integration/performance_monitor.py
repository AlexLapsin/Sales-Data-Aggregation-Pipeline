#!/usr/bin/env python3
"""
Performance Monitoring for End-to-End Pipeline Testing

This module provides comprehensive performance monitoring capabilities
for all stages of the sales data pipeline, including throughput measurement,
resource utilization tracking, and bottleneck identification.
"""

import asyncio
import logging
import psutil
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import json
from pathlib import Path
import numpy as np
import pandas as pd


@dataclass
class PerformanceMetric:
    """Individual performance metric measurement"""

    timestamp: datetime
    metric_name: str
    value: float
    unit: str
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "metric_name": self.metric_name,
            "value": self.value,
            "unit": self.unit,
            "tags": self.tags,
        }


@dataclass
class PerformanceSnapshot:
    """Performance snapshot at a point in time"""

    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_sent_mb: float
    network_recv_mb: float
    process_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "memory_used_mb": self.memory_used_mb,
            "disk_io_read_mb": self.disk_io_read_mb,
            "disk_io_write_mb": self.disk_io_write_mb,
            "network_sent_mb": self.network_sent_mb,
            "network_recv_mb": self.network_recv_mb,
            "process_count": self.process_count,
        }


@dataclass
class ComponentPerformanceReport:
    """Performance report for a specific pipeline component"""

    component_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    metrics: List[PerformanceMetric]
    snapshots: List[PerformanceSnapshot]
    summary_stats: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component_name": self.component_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": self.duration_seconds,
            "metrics": [m.to_dict() for m in self.metrics],
            "snapshots": [s.to_dict() for s in self.snapshots],
            "summary_stats": self.summary_stats,
        }


class PerformanceMonitor:
    """Comprehensive performance monitoring for pipeline components"""

    def __init__(self, sampling_interval: float = 1.0):
        """
        Initialize performance monitor

        Args:
            sampling_interval: Seconds between performance samples
        """
        self.sampling_interval = sampling_interval
        self.logger = logging.getLogger(__name__)

        # Performance tracking
        self.active_monitors = {}
        self.baseline_metrics = None

        # Resource tracking
        self._last_disk_io = None
        self._last_network_io = None

    def capture_baseline(self) -> PerformanceSnapshot:
        """Capture baseline system performance"""

        snapshot = self._capture_system_snapshot()
        self.baseline_metrics = snapshot

        self.logger.info("Baseline performance captured")
        return snapshot

    def _capture_system_snapshot(self) -> PerformanceSnapshot:
        """Capture current system performance snapshot"""

        # CPU and memory
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()

        # Disk I/O
        disk_io = psutil.disk_io_counters()
        disk_read_mb = disk_io.read_bytes / (1024 * 1024) if disk_io else 0
        disk_write_mb = disk_io.write_bytes / (1024 * 1024) if disk_io else 0

        # Network I/O
        network_io = psutil.net_io_counters()
        network_sent_mb = network_io.bytes_sent / (1024 * 1024) if network_io else 0
        network_recv_mb = network_io.bytes_recv / (1024 * 1024) if network_io else 0

        # Process count
        process_count = len(psutil.pids())

        return PerformanceSnapshot(
            timestamp=datetime.now(),
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_used_mb=memory.used / (1024 * 1024),
            disk_io_read_mb=disk_read_mb,
            disk_io_write_mb=disk_write_mb,
            network_sent_mb=network_sent_mb,
            network_recv_mb=network_recv_mb,
            process_count=process_count,
        )

    async def monitor_component(
        self,
        component_name: str,
        duration_seconds: Optional[float] = None,
        process_names: Optional[List[str]] = None,
    ) -> ComponentPerformanceReport:
        """
        Monitor performance of a specific component

        Args:
            component_name: Name of the component being monitored
            duration_seconds: How long to monitor (None for manual stop)
            process_names: Specific process names to monitor
        """

        self.logger.info(f"Starting performance monitoring for: {component_name}")

        start_time = datetime.now()
        metrics = []
        snapshots = []

        # Start monitoring task
        monitor_task = asyncio.create_task(
            self._monitor_loop(component_name, metrics, snapshots, process_names)
        )

        # Wait for specified duration or until manually stopped
        if duration_seconds:
            await asyncio.sleep(duration_seconds)
            monitor_task.cancel()

            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        else:
            # Monitor until manually stopped
            self.active_monitors[component_name] = monitor_task
            await monitor_task

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Calculate summary statistics
        summary_stats = self._calculate_summary_stats(metrics, snapshots)

        report = ComponentPerformanceReport(
            component_name=component_name,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            metrics=metrics,
            snapshots=snapshots,
            summary_stats=summary_stats,
        )

        self.logger.info(f"Performance monitoring complete for: {component_name}")
        return report

    async def _monitor_loop(
        self,
        component_name: str,
        metrics: List[PerformanceMetric],
        snapshots: List[PerformanceSnapshot],
        process_names: Optional[List[str]] = None,
    ):
        """Main monitoring loop"""

        try:
            while True:
                # Capture system snapshot
                snapshot = self._capture_system_snapshot()
                snapshots.append(snapshot)

                # Monitor specific processes if specified
                if process_names:
                    process_metrics = self._monitor_processes(process_names)
                    metrics.extend(process_metrics)

                await asyncio.sleep(self.sampling_interval)

        except asyncio.CancelledError:
            self.logger.debug(f"Monitoring cancelled for: {component_name}")
            raise

    def _monitor_processes(self, process_names: List[str]) -> List[PerformanceMetric]:
        """Monitor specific processes by name"""

        metrics = []
        timestamp = datetime.now()

        for proc in psutil.process_iter(["pid", "name", "cpu_percent", "memory_info"]):
            try:
                if proc.info["name"] in process_names:
                    # CPU metric
                    cpu_metric = PerformanceMetric(
                        timestamp=timestamp,
                        metric_name="process_cpu_percent",
                        value=proc.info["cpu_percent"] or 0,
                        unit="percent",
                        tags={
                            "process_name": proc.info["name"],
                            "pid": str(proc.info["pid"]),
                        },
                    )
                    metrics.append(cpu_metric)

                    # Memory metric
                    memory_mb = (
                        proc.info["memory_info"].rss / (1024 * 1024)
                        if proc.info["memory_info"]
                        else 0
                    )
                    memory_metric = PerformanceMetric(
                        timestamp=timestamp,
                        metric_name="process_memory_mb",
                        value=memory_mb,
                        unit="MB",
                        tags={
                            "process_name": proc.info["name"],
                            "pid": str(proc.info["pid"]),
                        },
                    )
                    metrics.append(memory_metric)

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        return metrics

    def _calculate_summary_stats(
        self, metrics: List[PerformanceMetric], snapshots: List[PerformanceSnapshot]
    ) -> Dict[str, Any]:
        """Calculate summary statistics from monitoring data"""

        if not snapshots:
            return {}

        # Convert snapshots to DataFrame for easier analysis
        snapshot_data = [s.to_dict() for s in snapshots]
        df = pd.DataFrame(snapshot_data)

        summary = {
            "cpu": {
                "avg_percent": df["cpu_percent"].mean(),
                "max_percent": df["cpu_percent"].max(),
                "min_percent": df["cpu_percent"].min(),
                "std_percent": df["cpu_percent"].std(),
            },
            "memory": {
                "avg_percent": df["memory_percent"].mean(),
                "max_percent": df["memory_percent"].max(),
                "min_percent": df["memory_percent"].min(),
                "avg_used_mb": df["memory_used_mb"].mean(),
                "max_used_mb": df["memory_used_mb"].max(),
            },
            "disk_io": {
                "total_read_mb": (
                    df["disk_io_read_mb"].iloc[-1] - df["disk_io_read_mb"].iloc[0]
                    if len(df) > 1
                    else 0
                ),
                "total_write_mb": (
                    df["disk_io_write_mb"].iloc[-1] - df["disk_io_write_mb"].iloc[0]
                    if len(df) > 1
                    else 0
                ),
                "avg_read_rate_mb_s": self._calculate_rate(df, "disk_io_read_mb"),
                "avg_write_rate_mb_s": self._calculate_rate(df, "disk_io_write_mb"),
            },
            "network": {
                "total_sent_mb": (
                    df["network_sent_mb"].iloc[-1] - df["network_sent_mb"].iloc[0]
                    if len(df) > 1
                    else 0
                ),
                "total_recv_mb": (
                    df["network_recv_mb"].iloc[-1] - df["network_recv_mb"].iloc[0]
                    if len(df) > 1
                    else 0
                ),
                "avg_send_rate_mb_s": self._calculate_rate(df, "network_sent_mb"),
                "avg_recv_rate_mb_s": self._calculate_rate(df, "network_recv_mb"),
            },
            "sampling": {
                "total_samples": len(snapshots),
                "duration_seconds": (
                    (snapshots[-1].timestamp - snapshots[0].timestamp).total_seconds()
                    if len(snapshots) > 1
                    else 0
                ),
                "sampling_rate": (
                    len(snapshots)
                    / max(
                        1,
                        (
                            snapshots[-1].timestamp - snapshots[0].timestamp
                        ).total_seconds(),
                    )
                    if len(snapshots) > 1
                    else 0
                ),
            },
        }

        # Add process-specific metrics if available
        if metrics:
            process_stats = self._calculate_process_stats(metrics)
            summary["processes"] = process_stats

        return summary

    def _calculate_rate(self, df: pd.DataFrame, column: str) -> float:
        """Calculate average rate of change for a column"""

        if len(df) < 2:
            return 0.0

        # Convert timestamps to seconds for rate calculation
        df_copy = df.copy()
        df_copy["timestamp"] = pd.to_datetime(df_copy["timestamp"])
        df_copy = df_copy.sort_values("timestamp")

        time_diff = (
            df_copy["timestamp"].iloc[-1] - df_copy["timestamp"].iloc[0]
        ).total_seconds()
        value_diff = df_copy[column].iloc[-1] - df_copy[column].iloc[0]

        return value_diff / max(1, time_diff)

    def _calculate_process_stats(
        self, metrics: List[PerformanceMetric]
    ) -> Dict[str, Any]:
        """Calculate statistics for process-specific metrics"""

        process_stats = {}

        # Group metrics by process
        process_metrics = {}
        for metric in metrics:
            process_name = metric.tags.get("process_name", "unknown")
            if process_name not in process_metrics:
                process_metrics[process_name] = {"cpu": [], "memory": []}

            if metric.metric_name == "process_cpu_percent":
                process_metrics[process_name]["cpu"].append(metric.value)
            elif metric.metric_name == "process_memory_mb":
                process_metrics[process_name]["memory"].append(metric.value)

        # Calculate stats for each process
        for process_name, data in process_metrics.items():
            if data["cpu"]:
                cpu_stats = {
                    "avg_cpu_percent": np.mean(data["cpu"]),
                    "max_cpu_percent": np.max(data["cpu"]),
                    "min_cpu_percent": np.min(data["cpu"]),
                }
            else:
                cpu_stats = {}

            if data["memory"]:
                memory_stats = {
                    "avg_memory_mb": np.mean(data["memory"]),
                    "max_memory_mb": np.max(data["memory"]),
                    "min_memory_mb": np.min(data["memory"]),
                }
            else:
                memory_stats = {}

            process_stats[process_name] = {**cpu_stats, **memory_stats}

        return process_stats

    def stop_monitoring(self, component_name: str):
        """Stop monitoring a specific component"""

        if component_name in self.active_monitors:
            self.active_monitors[component_name].cancel()
            del self.active_monitors[component_name]
            self.logger.info(f"Stopped monitoring: {component_name}")

    def stop_all_monitoring(self):
        """Stop all active monitoring"""

        for component_name in list(self.active_monitors.keys()):
            self.stop_monitoring(component_name)

    # Specialized monitoring methods for pipeline components

    async def monitor_kafka_streaming(
        self, bootstrap_servers: str, topic: str, duration_seconds: float = 60
    ) -> ComponentPerformanceReport:
        """Monitor Kafka streaming performance"""

        self.logger.info(f"Monitoring Kafka streaming for topic: {topic}")

        # Monitor Kafka-related processes
        kafka_processes = ["kafka", "java"]  # Common Kafka process names

        return await self.monitor_component(
            component_name="kafka_streaming",
            duration_seconds=duration_seconds,
            process_names=kafka_processes,
        )

    async def monitor_spark_processing(
        self, duration_seconds: Optional[float] = None
    ) -> ComponentPerformanceReport:
        """Monitor Spark ETL processing performance"""

        self.logger.info("Monitoring Spark processing")

        # Monitor Spark-related processes
        spark_processes = ["java", "python", "spark"]  # Common Spark process names

        return await self.monitor_component(
            component_name="spark_processing",
            duration_seconds=duration_seconds,
            process_names=spark_processes,
        )

    async def monitor_dbt_transformations(
        self, duration_seconds: Optional[float] = None
    ) -> ComponentPerformanceReport:
        """Monitor dbt transformation performance"""

        self.logger.info("Monitoring dbt transformations")

        # Monitor dbt-related processes
        dbt_processes = ["python", "dbt"]  # Common dbt process names

        return await self.monitor_component(
            component_name="dbt_transformations",
            duration_seconds=duration_seconds,
            process_names=dbt_processes,
        )

    async def monitor_database_operations(
        self, duration_seconds: Optional[float] = None
    ) -> ComponentPerformanceReport:
        """Monitor database operations performance"""

        self.logger.info("Monitoring database operations")

        # Monitor database-related processes
        db_processes = [
            "postgres",
            "mysqld",
            "snowflake",
        ]  # Common database process names

        return await self.monitor_component(
            component_name="database_operations",
            duration_seconds=duration_seconds,
            process_names=db_processes,
        )

    # Analysis and reporting methods

    def analyze_performance_trends(
        self, reports: List[ComponentPerformanceReport]
    ) -> Dict[str, Any]:
        """Analyze performance trends across multiple reports"""

        if not reports:
            return {}

        analysis = {
            "component_comparison": {},
            "resource_utilization": {},
            "performance_trends": {},
            "bottleneck_analysis": {},
        }

        # Component comparison
        for report in reports:
            component_name = report.component_name
            summary = report.summary_stats

            analysis["component_comparison"][component_name] = {
                "duration_seconds": report.duration_seconds,
                "avg_cpu_percent": summary.get("cpu", {}).get("avg_percent", 0),
                "max_cpu_percent": summary.get("cpu", {}).get("max_percent", 0),
                "avg_memory_percent": summary.get("memory", {}).get("avg_percent", 0),
                "max_memory_mb": summary.get("memory", {}).get("max_used_mb", 0),
                "total_disk_io_mb": (
                    summary.get("disk_io", {}).get("total_read_mb", 0)
                    + summary.get("disk_io", {}).get("total_write_mb", 0)
                ),
                "total_network_io_mb": (
                    summary.get("network", {}).get("total_sent_mb", 0)
                    + summary.get("network", {}).get("total_recv_mb", 0)
                ),
            }

        # Overall resource utilization
        all_cpu = [
            comp["avg_cpu_percent"]
            for comp in analysis["component_comparison"].values()
        ]
        all_memory = [
            comp["avg_memory_percent"]
            for comp in analysis["component_comparison"].values()
        ]

        analysis["resource_utilization"] = {
            "avg_cpu_across_components": np.mean(all_cpu) if all_cpu else 0,
            "max_cpu_across_components": np.max(all_cpu) if all_cpu else 0,
            "avg_memory_across_components": np.mean(all_memory) if all_memory else 0,
            "max_memory_across_components": np.max(all_memory) if all_memory else 0,
        }

        # Bottleneck analysis
        bottlenecks = []

        for component_name, stats in analysis["component_comparison"].items():
            # CPU bottleneck
            if stats["avg_cpu_percent"] > 80:
                bottlenecks.append(
                    {
                        "component": component_name,
                        "type": "cpu",
                        "severity": (
                            "high" if stats["avg_cpu_percent"] > 90 else "medium"
                        ),
                        "value": stats["avg_cpu_percent"],
                    }
                )

            # Memory bottleneck
            if stats["avg_memory_percent"] > 80:
                bottlenecks.append(
                    {
                        "component": component_name,
                        "type": "memory",
                        "severity": (
                            "high" if stats["avg_memory_percent"] > 90 else "medium"
                        ),
                        "value": stats["avg_memory_percent"],
                    }
                )

        analysis["bottleneck_analysis"] = {
            "bottlenecks_found": len(bottlenecks),
            "bottlenecks": bottlenecks,
        }

        return analysis

    def generate_performance_report(
        self, reports: List[ComponentPerformanceReport], output_path: str
    ) -> str:
        """Generate comprehensive performance report"""

        report_data = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "components_monitored": len(reports),
                "total_monitoring_duration": sum(r.duration_seconds for r in reports),
            },
            "component_reports": [r.to_dict() for r in reports],
            "performance_analysis": self.analyze_performance_trends(reports),
        }

        # Save report
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        self.logger.info(f"Performance report saved to: {output_path}")
        return str(output_file)

    def identify_performance_regressions(
        self,
        current_reports: List[ComponentPerformanceReport],
        baseline_reports: List[ComponentPerformanceReport],
        threshold_percent: float = 20.0,
    ) -> Dict[str, Any]:
        """Identify performance regressions compared to baseline"""

        regressions = []
        improvements = []

        # Create lookup for baseline reports
        baseline_lookup = {r.component_name: r for r in baseline_reports}

        for current_report in current_reports:
            component_name = current_report.component_name

            if component_name not in baseline_lookup:
                continue

            baseline_report = baseline_lookup[component_name]

            # Compare key metrics
            current_stats = current_report.summary_stats
            baseline_stats = baseline_report.summary_stats

            # Duration comparison
            current_duration = current_report.duration_seconds
            baseline_duration = baseline_report.duration_seconds
            duration_change = (
                (current_duration - baseline_duration) / baseline_duration
            ) * 100

            if abs(duration_change) > threshold_percent:
                change_data = {
                    "component": component_name,
                    "metric": "duration",
                    "current_value": current_duration,
                    "baseline_value": baseline_duration,
                    "change_percent": duration_change,
                }

                if duration_change > 0:
                    regressions.append(change_data)
                else:
                    improvements.append(change_data)

            # CPU comparison
            current_cpu = current_stats.get("cpu", {}).get("avg_percent", 0)
            baseline_cpu = baseline_stats.get("cpu", {}).get("avg_percent", 0)

            if baseline_cpu > 0:
                cpu_change = ((current_cpu - baseline_cpu) / baseline_cpu) * 100

                if abs(cpu_change) > threshold_percent:
                    change_data = {
                        "component": component_name,
                        "metric": "cpu_usage",
                        "current_value": current_cpu,
                        "baseline_value": baseline_cpu,
                        "change_percent": cpu_change,
                    }

                    if cpu_change > 0:
                        regressions.append(change_data)
                    else:
                        improvements.append(change_data)

            # Memory comparison
            current_memory = current_stats.get("memory", {}).get("avg_percent", 0)
            baseline_memory = baseline_stats.get("memory", {}).get("avg_percent", 0)

            if baseline_memory > 0:
                memory_change = (
                    (current_memory - baseline_memory) / baseline_memory
                ) * 100

                if abs(memory_change) > threshold_percent:
                    change_data = {
                        "component": component_name,
                        "metric": "memory_usage",
                        "current_value": current_memory,
                        "baseline_value": baseline_memory,
                        "change_percent": memory_change,
                    }

                    if memory_change > 0:
                        regressions.append(change_data)
                    else:
                        improvements.append(change_data)

        return {
            "regressions": regressions,
            "improvements": improvements,
            "regression_count": len(regressions),
            "improvement_count": len(improvements),
            "threshold_percent": threshold_percent,
        }


class BenchmarkRunner:
    """Runs performance benchmarks for pipeline components"""

    def __init__(self, monitor: PerformanceMonitor):
        self.monitor = monitor
        self.logger = logging.getLogger(__name__)

    async def run_throughput_benchmark(
        self, component_name: str, workload_sizes: List[int], **kwargs
    ) -> Dict[str, Any]:
        """Run throughput benchmark with different workload sizes"""

        self.logger.info(f"Running throughput benchmark for: {component_name}")

        results = []

        for workload_size in workload_sizes:
            self.logger.info(f"Testing workload size: {workload_size}")

            # Run component with specific workload size
            start_time = time.time()

            # This would need to be implemented specific to each component
            # For now, simulate processing time
            processing_time = workload_size / 1000  # Simulate 1000 records/second
            await asyncio.sleep(
                min(processing_time, 30)
            )  # Cap at 30 seconds for testing

            end_time = time.time()
            actual_duration = end_time - start_time

            throughput = workload_size / actual_duration if actual_duration > 0 else 0

            results.append(
                {
                    "workload_size": workload_size,
                    "duration_seconds": actual_duration,
                    "throughput": throughput,
                    "throughput_unit": "records_per_second",
                }
            )

            self.logger.info(f"Workload {workload_size}: {throughput:.2f} records/sec")

        # Calculate benchmark summary
        throughputs = [r["throughput"] for r in results]

        benchmark_result = {
            "component_name": component_name,
            "workload_sizes": workload_sizes,
            "results": results,
            "summary": {
                "min_throughput": min(throughputs) if throughputs else 0,
                "max_throughput": max(throughputs) if throughputs else 0,
                "avg_throughput": np.mean(throughputs) if throughputs else 0,
                "throughput_consistency": np.std(throughputs) if throughputs else 0,
            },
        }

        return benchmark_result

    async def run_scalability_test(
        self, component_name: str, max_workload: int, step_size: int = 1000
    ) -> Dict[str, Any]:
        """Test component scalability with increasing workloads"""

        workload_sizes = list(range(step_size, max_workload + 1, step_size))
        return await self.run_throughput_benchmark(component_name, workload_sizes)

    async def run_stress_test(
        self, component_name: str, stress_workload: int, duration_seconds: float = 300
    ) -> Dict[str, Any]:
        """Run stress test with sustained high workload"""

        self.logger.info(f"Running stress test for: {component_name}")

        # Start performance monitoring
        monitor_task = asyncio.create_task(
            self.monitor.monitor_component(component_name, duration_seconds)
        )

        # Simulate sustained workload
        start_time = time.time()

        # This would need to be implemented specific to each component
        # For now, simulate sustained processing
        await asyncio.sleep(duration_seconds)

        end_time = time.time()
        actual_duration = end_time - start_time

        # Get monitoring results
        performance_report = await monitor_task

        stress_result = {
            "component_name": component_name,
            "stress_workload": stress_workload,
            "planned_duration": duration_seconds,
            "actual_duration": actual_duration,
            "performance_report": performance_report.to_dict(),
            "stress_metrics": {
                "avg_cpu_percent": performance_report.summary_stats.get("cpu", {}).get(
                    "avg_percent", 0
                ),
                "max_cpu_percent": performance_report.summary_stats.get("cpu", {}).get(
                    "max_percent", 0
                ),
                "avg_memory_percent": performance_report.summary_stats.get(
                    "memory", {}
                ).get("avg_percent", 0),
                "max_memory_percent": performance_report.summary_stats.get(
                    "memory", {}
                ).get("max_percent", 0),
            },
        }

        return stress_result
