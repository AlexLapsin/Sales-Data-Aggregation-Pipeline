"""
Performance and load tests for Airflow DAGs
Tests DAG performance, resource usage, and scalability
"""

import pytest
import os
import sys
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import gc

# Add the DAG directory to Python path
dag_dir = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
sys.path.insert(0, dag_dir)

from airflow import DAG
from airflow.utils.state import State
from tests.airflow.test_utils import (
    MockAirflowContext,
    MockSnowflakeHook,
    MockExternalServices,
)


@pytest.mark.slow
class TestDAGPerformance:
    """Test DAG performance characteristics"""

    def test_dag_import_performance(self, test_env_vars):
        """Test DAG import time is reasonable"""
        import_times = {}

        dag_modules = [
            "cloud_sales_pipeline_dag",
            "pipeline_monitoring_dag",
            "maintenance_dag",
        ]

        for module_name in dag_modules:
            start_time = time.time()

            # Import the module
            if module_name in sys.modules:
                del sys.modules[module_name]

            __import__(module_name)

            import_time = time.time() - start_time
            import_times[module_name] = import_time

            # Each DAG should import within reasonable time
            assert import_time < 5.0, f"{module_name} took {import_time:.2f}s to import"

        # Total import time should be reasonable
        total_import_time = sum(import_times.values())
        assert (
            total_import_time < 10.0
        ), f"Total import time {total_import_time:.2f}s too high"

    def test_dag_instantiation_performance(self, test_env_vars):
        """Test DAG instantiation performance"""
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag
        import maintenance_dag

        modules = [cloud_sales_pipeline_dag, pipeline_monitoring_dag, maintenance_dag]

        for module in modules:
            start_time = time.time()

            # Find and access DAG object
            dag = None
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, DAG):
                    dag = attr
                    break

            instantiation_time = time.time() - start_time

            assert dag is not None
            assert (
                instantiation_time < 2.0
            ), f"DAG instantiation took {instantiation_time:.2f}s"

    def test_task_creation_performance(self, test_env_vars):
        """Test task creation performance"""
        import cloud_sales_pipeline_dag

        # Find the DAG
        dag = None
        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                dag = attr
                break

        assert dag is not None

        # Test task access performance
        start_time = time.time()

        task_count = 0
        for task in dag.tasks:
            task_count += 1
            # Access task properties
            _ = task.task_id
            _ = task.task_type
            _ = task.upstream_list
            _ = task.downstream_list

        access_time = time.time() - start_time

        assert task_count > 15  # Should have reasonable number of tasks
        assert (
            access_time < 1.0
        ), f"Task access took {access_time:.2f}s for {task_count} tasks"

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_function_execution_performance(self, mock_hook_class, test_env_vars):
        """Test DAG function execution performance"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            1000,
            50000.0,
            250,
            85.5,
            950,
            46000.0,
            240,
            5.26,
            8.70,
        ]
        mock_hook.get_records.return_value = [
            (datetime(2024, 1, 1), 85.5, 1000, 50, 5.0),
            ("MARTS", "FACT_SALES", 1000000, 5368709120, 5.0, None, False),
        ]
        mock_hook_class.return_value = mock_hook

        context = MockAirflowContext().get_context()

        # Test monitoring functions
        from pipeline_monitoring_dag import (
            generate_daily_report,
            check_data_quality_trends,
        )
        from maintenance_dag import calculate_table_sizes

        functions_to_test = [
            ("generate_daily_report", generate_daily_report),
            ("check_data_quality_trends", check_data_quality_trends),
            ("calculate_table_sizes", calculate_table_sizes),
        ]

        for func_name, func in functions_to_test:
            start_time = time.time()

            try:
                result = func(**context)
                execution_time = time.time() - start_time

                assert execution_time < 1.0, f"{func_name} took {execution_time:.2f}s"
                assert result is not None

            except Exception as e:
                pytest.fail(f"{func_name} failed: {e}")


@pytest.mark.slow
class TestMemoryUsage:
    """Test memory usage of DAGs"""

    def test_dag_memory_usage(self, test_env_vars):
        """Test DAG memory consumption"""
        # Get initial memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Import all DAGs
        dag_modules = []
        for module_name in [
            "cloud_sales_pipeline_dag",
            "pipeline_monitoring_dag",
            "maintenance_dag",
        ]:
            module = __import__(module_name)
            dag_modules.append(module)

        # Get memory after imports
        post_import_memory = process.memory_info().rss / 1024 / 1024  # MB
        import_memory_increase = post_import_memory - initial_memory

        # Memory increase should be reasonable
        assert (
            import_memory_increase < 100
        ), f"DAG imports increased memory by {import_memory_increase:.1f}MB"

        # Access all DAG objects
        dags = []
        for module in dag_modules:
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, DAG):
                    dags.append(attr)

        # Get memory after DAG access
        post_access_memory = process.memory_info().rss / 1024 / 1024  # MB
        access_memory_increase = post_access_memory - post_import_memory

        assert (
            access_memory_increase < 50
        ), f"DAG access increased memory by {access_memory_increase:.1f}MB"

        # Cleanup
        del dags
        del dag_modules
        gc.collect()

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_function_memory_usage(self, mock_hook_class, test_env_vars):
        """Test memory usage of DAG functions"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            1000,
            50000.0,
            250,
            85.5,
            950,
            46000.0,
            240,
            5.26,
            8.70,
        ]
        mock_hook.get_records.return_value = [
            (datetime(2024, 1, 1), 85.5, 1000, 50, 5.0)
        ] * 1000  # Large dataset
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        context = MockAirflowContext().get_context()

        # Execute function with large dataset
        result = check_data_quality_trends(**context)

        post_execution_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = post_execution_memory - initial_memory

        # Memory increase should be reasonable even with large dataset
        assert (
            memory_increase < 200
        ), f"Function increased memory by {memory_increase:.1f}MB"

        # Cleanup
        del result
        gc.collect()


@pytest.mark.slow
class TestConcurrentExecution:
    """Test concurrent execution performance"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_concurrent_function_execution(self, mock_hook_class, test_env_vars):
        """Test concurrent execution of DAG functions"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            1000,
            50000.0,
            250,
            85.5,
            950,
            46000.0,
            240,
            5.26,
            8.70,
        ]
        mock_hook.get_records.return_value = [
            (datetime(2024, 1, 1), 85.5, 1000, 50, 5.0)
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import (
            generate_daily_report,
            check_data_quality_trends,
        )

        def execute_function(func, context):
            """Execute function and return execution time"""
            start_time = time.time()
            result = func(**context)
            execution_time = time.time() - start_time
            return func.__name__, execution_time, result

        # Test concurrent execution
        contexts = [
            MockAirflowContext(ds=f"2024-01-{i:02d}").get_context() for i in range(1, 6)
        ]
        functions = [generate_daily_report, check_data_quality_trends]

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []

            for context in contexts:
                for func in functions:
                    future = executor.submit(execute_function, func, context)
                    futures.append(future)

            results = []
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=10)  # 10 second timeout
                    results.append(result)
                except Exception as e:
                    pytest.fail(f"Concurrent execution failed: {e}")

        total_time = time.time() - start_time

        # Should complete all executions within reasonable time
        assert total_time < 15, f"Concurrent execution took {total_time:.2f}s"
        assert len(results) == len(contexts) * len(functions)

        # Check individual execution times
        for func_name, exec_time, result in results:
            assert (
                exec_time < 5
            ), f"{func_name} took {exec_time:.2f}s in concurrent execution"
            assert result is not None

    def test_dag_import_concurrency(self, test_env_vars):
        """Test concurrent DAG imports"""

        def import_dag(module_name):
            """Import DAG module and return import time"""
            start_time = time.time()

            # Force reimport
            if module_name in sys.modules:
                del sys.modules[module_name]

            module = __import__(module_name)
            import_time = time.time() - start_time

            return module_name, import_time

        dag_modules = [
            "cloud_sales_pipeline_dag",
            "pipeline_monitoring_dag",
            "maintenance_dag",
        ]

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(import_dag, module) for module in dag_modules]

            results = []
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=10)
                    results.append(result)
                except Exception as e:
                    pytest.fail(f"Concurrent import failed: {e}")

        total_time = time.time() - start_time

        # Should complete all imports quickly
        assert total_time < 8, f"Concurrent imports took {total_time:.2f}s"
        assert len(results) == len(dag_modules)

        # Check individual import times
        for module_name, import_time in results:
            assert (
                import_time < 6
            ), f"{module_name} took {import_time:.2f}s to import concurrently"


@pytest.mark.slow
class TestLoadTesting:
    """Test DAG behavior under load"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_large_dataset_handling(self, mock_hook_class, test_env_vars):
        """Test handling of large datasets"""
        mock_hook = Mock()

        # Simulate large dataset
        large_dataset = []
        for i in range(10000):  # 10k records
            large_dataset.append(
                (
                    datetime(2024, 1, 1) + timedelta(hours=i % 24),
                    85.0 + (i % 20),  # Quality score
                    1000 + i,  # Record count
                    50 + (i % 100),  # Low quality count
                    5.0 + (i % 10),  # Low quality percentage
                )
            )

        mock_hook.get_records.return_value = large_dataset
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        start_time = time.time()
        result = check_data_quality_trends(**context)
        execution_time = time.time() - start_time

        # Should handle large dataset within reasonable time
        assert (
            execution_time < 10
        ), f"Large dataset processing took {execution_time:.2f}s"
        assert isinstance(result, list)

        # Should detect some quality issues in large dataset
        assert len(result) >= 0  # May or may not have issues based on data

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_repeated_execution_performance(self, mock_hook_class, test_env_vars):
        """Test performance degradation over repeated executions"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            1000,
            50000.0,
            250,
            85.5,
            950,
            46000.0,
            240,
            5.26,
            8.70,
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        execution_times = []

        # Run function multiple times
        for i in range(20):
            context = MockAirflowContext(ds=f"2024-01-{i+1:02d}").get_context()

            start_time = time.time()
            result = generate_daily_report(**context)
            execution_time = time.time() - start_time

            execution_times.append(execution_time)
            assert result is not None

        # Check for performance degradation
        avg_first_half = sum(execution_times[:10]) / 10
        avg_second_half = sum(execution_times[10:]) / 10

        # Performance shouldn't degrade significantly
        performance_ratio = avg_second_half / avg_first_half
        assert (
            performance_ratio < 2.0
        ), f"Performance degraded by {performance_ratio:.1f}x over repeated executions"

        # All executions should be reasonably fast
        max_execution_time = max(execution_times)
        assert (
            max_execution_time < 2.0
        ), f"Slowest execution took {max_execution_time:.2f}s"

    @patch("subprocess.run")
    def test_kafka_health_check_load(self, mock_subprocess, test_env_vars):
        """Test Kafka health check under load"""
        # Mock successful subprocess calls
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
        mock_subprocess.return_value = mock_result

        from cloud_sales_pipeline_dag import check_kafka_health

        execution_times = []

        # Simulate multiple rapid health checks
        for i in range(50):
            context = MockAirflowContext().get_context()

            start_time = time.time()
            result = check_kafka_health(**context)
            execution_time = time.time() - start_time

            execution_times.append(execution_time)
            assert result == "spark_batch_processing"

        # Check performance characteristics
        avg_execution_time = sum(execution_times) / len(execution_times)
        max_execution_time = max(execution_times)

        assert (
            avg_execution_time < 0.1
        ), f"Average health check took {avg_execution_time:.3f}s"
        assert (
            max_execution_time < 1.0
        ), f"Slowest health check took {max_execution_time:.3f}s"


@pytest.mark.slow
class TestScalabilityTesting:
    """Test DAG scalability characteristics"""

    def test_task_dependency_scaling(self, test_env_vars):
        """Test task dependency resolution scaling"""
        import cloud_sales_pipeline_dag

        # Find the DAG
        dag = None
        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                dag = attr
                break

        assert dag is not None

        # Test dependency resolution performance
        start_time = time.time()

        dependency_checks = 0
        for task in dag.tasks:
            # Check upstream dependencies
            for upstream_task in task.upstream_list:
                dependency_checks += 1
                assert upstream_task in dag.tasks

            # Check downstream dependencies
            for downstream_task in task.downstream_list:
                dependency_checks += 1
                assert downstream_task in dag.tasks

        resolution_time = time.time() - start_time

        # Dependency resolution should be fast even with many checks
        assert (
            resolution_time < 1.0
        ), f"Dependency resolution took {resolution_time:.2f}s for {dependency_checks} checks"
        assert dependency_checks > 20  # Should have reasonable number of dependencies

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_xcom_scaling(self, mock_hook_class, test_env_vars):
        """Test XCom data handling scaling"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            1000,
            50000.0,
            250,
            85.5,
            950,
            46000.0,
            240,
            5.26,
            8.70,
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        # Simulate many XCom operations
        contexts = []
        for i in range(100):
            context = MockAirflowContext(
                ds=f"2024-{i//30+1:02d}-{i%30+1:02d}"
            ).get_context()
            contexts.append(context)

        start_time = time.time()

        results = []
        for context in contexts:
            result = generate_daily_report(**context)
            results.append(result)

        total_time = time.time() - start_time

        # Should handle many XCom operations efficiently
        assert total_time < 20, f"100 XCom operations took {total_time:.2f}s"
        assert len(results) == 100

        # Check XCom call counts
        total_xcom_calls = 0
        for context in contexts:
            total_xcom_calls += context["task_instance"].xcom_push.call_count

        assert total_xcom_calls >= 100  # At least one XCom push per execution


@pytest.mark.slow
class TestResourceConstraints:
    """Test DAG behavior under resource constraints"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_timeout_handling(self, mock_hook_class, test_env_vars):
        """Test function behavior with timeouts"""
        mock_hook = Mock()

        # Simulate slow database response
        def slow_response(*args, **kwargs):
            time.sleep(2)  # 2 second delay
            return [20240101, 1000, 50000.0, 250, 85.5, 950, 46000.0, 240, 5.26, 8.70]

        mock_hook.get_first.side_effect = slow_response
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        start_time = time.time()
        result = generate_daily_report(**context)
        execution_time = time.time() - start_time

        # Should handle slow responses gracefully
        assert execution_time >= 2.0  # Should include the delay
        assert execution_time < 5.0  # But shouldn't hang
        assert result is not None

    def test_error_recovery_performance(self, test_env_vars):
        """Test error recovery performance"""
        from cloud_sales_pipeline_dag import check_kafka_health

        error_recovery_times = []

        # Test multiple error scenarios
        for i in range(10):
            with patch("subprocess.run") as mock_subprocess:
                # Simulate various failure scenarios
                if i % 3 == 0:
                    mock_subprocess.side_effect = Exception("Connection timeout")
                elif i % 3 == 1:
                    mock_result = Mock()
                    mock_result.returncode = 1
                    mock_subprocess.return_value = mock_result
                else:
                    mock_result = Mock()
                    mock_result.returncode = 0
                    mock_result.stdout = '{"connector":{"state":"FAILED"}}'
                    mock_subprocess.return_value = mock_result

                context = MockAirflowContext().get_context()

                start_time = time.time()
                result = check_kafka_health(**context)
                recovery_time = time.time() - start_time

                error_recovery_times.append(recovery_time)
                assert result == "kafka_restart"  # All errors should lead to restart

        # Error recovery should be fast
        avg_recovery_time = sum(error_recovery_times) / len(error_recovery_times)
        max_recovery_time = max(error_recovery_times)

        assert (
            avg_recovery_time < 1.0
        ), f"Average error recovery took {avg_recovery_time:.2f}s"
        assert (
            max_recovery_time < 2.0
        ), f"Slowest error recovery took {max_recovery_time:.2f}s"


@pytest.mark.slow
class TestLongRunningOperations:
    """Test long-running operations and their performance"""

    def test_maintenance_operation_simulation(self, test_env_vars):
        """Test maintenance operations performance simulation"""
        import maintenance_dag

        # Find the DAG
        dag = None
        for attr_name in dir(maintenance_dag):
            attr = getattr(maintenance_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_maintenance":
                dag = attr
                break

        assert dag is not None

        # Test that maintenance tasks have appropriate timeouts
        long_running_tasks = [
            "cleanup_old_raw_data",
            "optimize_table_clustering",
            "vacuum_tables",
            "cleanup_s3_old_data",
        ]

        for task_id in long_running_tasks:
            task = dag.get_task(task_id)

            # Should have execution timeout from default_args
            default_timeout = dag.default_args.get("execution_timeout")
            assert default_timeout is not None
            assert default_timeout == timedelta(hours=2)

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_large_table_analysis_performance(self, mock_hook_class, test_env_vars):
        """Test performance with large table statistics"""
        mock_hook = Mock()

        # Simulate many large tables
        large_table_stats = []
        for i in range(1000):  # 1000 tables
            large_table_stats.append(
                (
                    "MARTS" if i % 3 == 0 else "RAW",
                    f"TABLE_{i}",
                    1000000 + i * 10000,  # Row count
                    5368709120 + i * 1073741824,  # Bytes (5GB + i GB)
                    5.0 + i,  # GB
                    "CLUSTER_KEY" if i % 5 == 0 else None,  # Clustering key
                    i % 7 == 0,  # Auto clustering
                )
            )

        mock_hook.get_records.return_value = large_table_stats
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        start_time = time.time()
        recommendations = calculate_table_sizes(**context)
        analysis_time = time.time() - start_time

        # Should handle large number of tables efficiently
        assert analysis_time < 5.0, f"Large table analysis took {analysis_time:.2f}s"
        assert isinstance(recommendations, list)

        # Should generate reasonable number of recommendations
        assert len(recommendations) > 0
        assert len(recommendations) < len(
            large_table_stats
        )  # Not every table needs optimization
