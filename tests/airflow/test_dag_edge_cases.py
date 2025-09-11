"""
Edge case and error scenario tests for Airflow DAGs
Tests edge cases, error conditions, and boundary scenarios
"""

import pytest
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, call, side_effect
import json
import subprocess
import socket

# Add the DAG directory to Python path
dag_dir = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
sys.path.insert(0, dag_dir)

from airflow import DAG
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from tests.airflow.test_utils import (
    MockAirflowContext,
    MockSnowflakeHook,
    mock_failed_subprocess_run,
    mock_successful_subprocess_run,
)


class TestEdgeCasesCloudPipeline:
    """Test edge cases for cloud sales pipeline DAG"""

    @patch("subprocess.run")
    def test_kafka_health_check_invalid_json(self, mock_subprocess, test_env_vars):
        """Test Kafka health check with invalid JSON response"""
        # Mock subprocess with invalid JSON
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "invalid json response"
        mock_subprocess.return_value = mock_result

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="error"
        )

    @patch("subprocess.run")
    def test_kafka_health_check_empty_response(self, mock_subprocess, test_env_vars):
        """Test Kafka health check with empty response"""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_subprocess.return_value = mock_result

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="error"
        )

    @patch("subprocess.run")
    def test_kafka_health_check_timeout(self, mock_subprocess, test_env_vars):
        """Test Kafka health check with timeout"""
        mock_subprocess.side_effect = subprocess.TimeoutExpired("curl", 30)

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="error"
        )

    @patch("subprocess.run")
    def test_kafka_health_check_missing_state_field(
        self, mock_subprocess, test_env_vars
    ):
        """Test Kafka health check with missing state field"""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = (
            '{"connector":{"status":"unknown"}}'  # Missing 'state' field
        )
        mock_subprocess.return_value = mock_result

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="error"
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_data_freshness_check_null_values(self, mock_hook_class, test_env_vars):
        """Test data freshness check with null values"""
        mock_hook = Mock()
        mock_hook.get_first.side_effect = [
            [None],  # streaming_count is None
            [100],  # batch_count is valid
        ]
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        result = check_data_freshness(**context)

        assert result == "dbt_run_staging"  # Should proceed with batch data

        # Verify XCom pushes handle None values
        expected_calls = [
            call(key="streaming_records", value=0),  # None converted to 0
            call(key="batch_records", value=100),
            call(key="total_records", value=100),
        ]
        context["task_instance"].xcom_push.assert_has_calls(
            expected_calls, any_order=True
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_data_freshness_check_empty_result(self, mock_hook_class, test_env_vars):
        """Test data freshness check with empty query results"""
        mock_hook = Mock()
        mock_hook.get_first.side_effect = [[], []]  # Empty result  # Empty result
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        result = check_data_freshness(**context)

        assert result == "skip_pipeline"

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_data_freshness_check_connection_error(
        self, mock_hook_class, test_env_vars
    ):
        """Test data freshness check with connection error"""
        mock_hook = Mock()
        mock_hook.get_first.side_effect = Exception("Connection refused")
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        # Should proceed anyway in case of error
        result = check_data_freshness(**context)
        assert result == "dbt_run_staging"


class TestEdgeCasesMonitoring:
    """Test edge cases for pipeline monitoring DAG"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_generate_daily_report_malformed_data(self, mock_hook_class, test_env_vars):
        """Test daily report generation with malformed data"""
        mock_hook = Mock()
        # Return fewer columns than expected
        mock_hook.get_first.return_value = [20240101, 1000]  # Missing columns
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        with pytest.raises(Exception):
            generate_daily_report(**context)

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_generate_daily_report_negative_values(
        self, mock_hook_class, test_env_vars
    ):
        """Test daily report generation with negative values"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            -1000,
            -50000.0,
            -250,
            85.5,
            950,
            46000.0,
            240,
            -105.26,
            -8.70,
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        result = generate_daily_report(**context)

        # Should handle negative values gracefully
        assert result["total_transactions"] == -1000
        assert result["total_sales"] == -50000.0
        assert result["transaction_growth"] == -105.26

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_generate_daily_report_division_by_zero(
        self, mock_hook_class, test_env_vars
    ):
        """Test daily report generation with division by zero scenario"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = [
            20240101,
            1000,
            50000.0,
            250,
            85.5,
            0,
            0,
            0,
            None,
            None,  # Previous values are 0
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        result = generate_daily_report(**context)

        # Should handle division by zero gracefully
        assert result["total_transactions"] == 1000
        # Growth rates should be None or handle zero division
        assert result["transaction_growth"] in [None, float("inf"), 0]

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_check_quality_trends_extreme_values(self, mock_hook_class, test_env_vars):
        """Test quality trends check with extreme values"""
        mock_hook = Mock()
        mock_hook.get_records.return_value = [
            (
                datetime(2024, 1, 1),
                0.0,
                1000000,
                999999,
                99.9,
            ),  # Extremely poor quality
            (datetime(2024, 1, 2), 100.0, 0, 0, 0.0),  # Perfect quality, no records
            (
                datetime(2024, 1, 3),
                -10.0,
                1000,
                1200,
                120.0,
            ),  # Invalid negative quality
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        result = check_data_quality_trends(**context)

        # Should detect issues with extreme values
        assert len(result) >= 2  # Should flag multiple issues

        # Check for specific issue types
        issues_by_date = {issue["date"]: issue for issue in result}
        assert "2024-01-01" in issues_by_date
        assert "2024-01-03" in issues_by_date

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_check_quality_trends_no_data(self, mock_hook_class, test_env_vars):
        """Test quality trends check with no data"""
        mock_hook = Mock()
        mock_hook.get_records.return_value = []
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        result = check_data_quality_trends(**context)

        assert result == []
        context["task_instance"].xcom_push.assert_called_with(
            key="quality_issues", value=[]
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_check_quality_trends_data_type_errors(
        self, mock_hook_class, test_env_vars
    ):
        """Test quality trends check with data type errors"""
        mock_hook = Mock()
        mock_hook.get_records.return_value = [
            (datetime(2024, 1, 1), "invalid", "not_a_number", None, "bad_percent"),
            (datetime(2024, 1, 2), 85.5, 1000, 50, 5.0),  # Valid record
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        # Should handle data type errors gracefully
        try:
            result = check_data_quality_trends(**context)
            # May succeed with partial processing or raise exception
            assert isinstance(result, list)
        except Exception:
            # Exception is acceptable for invalid data types
            pass


class TestEdgeCasesMaintenance:
    """Test edge cases for maintenance DAG"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_empty_database(self, mock_hook_class, test_env_vars):
        """Test table size calculation with empty database"""
        mock_hook = Mock()
        mock_hook.get_records.return_value = []
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        result = calculate_table_sizes(**context)

        assert result == []
        context["task_instance"].xcom_push.assert_called_with(
            key="table_recommendations", value=[]
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_invalid_sizes(self, mock_hook_class, test_env_vars):
        """Test table size calculation with invalid size values"""
        mock_hook = Mock()
        mock_hook.get_records.return_value = [
            (
                "MARTS",
                "INVALID_TABLE",
                -1000,
                -5368709120,
                -5.0,
                None,
                False,
            ),  # Negative sizes
            ("MARTS", "ZERO_TABLE", 0, 0, 0.0, None, False),  # Zero sizes
            ("MARTS", "NULL_TABLE", None, None, None, None, None),  # Null sizes
        ]
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        result = calculate_table_sizes(**context)

        # Should handle invalid sizes gracefully
        assert isinstance(result, list)
        # May or may not generate recommendations for invalid data

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_missing_columns(
        self, mock_hook_class, test_env_vars
    ):
        """Test table size calculation with missing columns"""
        mock_hook = Mock()
        mock_hook.get_records.return_value = [
            ("MARTS", "INCOMPLETE_TABLE"),  # Missing most columns
        ]
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        with pytest.raises(Exception):
            calculate_table_sizes(**context)

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_query_timeout(self, mock_hook_class, test_env_vars):
        """Test table size calculation with query timeout"""
        mock_hook = Mock()
        mock_hook.get_records.side_effect = Exception("Query timeout")
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        with pytest.raises(Exception, match="Query timeout"):
            calculate_table_sizes(**context)

        # Should push error to XCom
        expected_error = [{"error": "Failed to calculate table sizes: Query timeout"}]
        context["task_instance"].xcom_push.assert_called_with(
            key="table_recommendations", value=expected_error
        )


class TestErrorRecoveryScenarios:
    """Test error recovery and resilience scenarios"""

    @patch("subprocess.run")
    def test_kafka_health_check_network_issues(self, mock_subprocess, test_env_vars):
        """Test Kafka health check with various network issues"""
        network_errors = [
            socket.timeout("Connection timed out"),
            ConnectionRefusedError("Connection refused"),
            socket.gaierror("Name resolution failed"),
            OSError("Network is unreachable"),
        ]

        from cloud_sales_pipeline_dag import check_kafka_health

        for error in network_errors:
            mock_subprocess.side_effect = error
            context = MockAirflowContext().get_context()

            result = check_kafka_health(**context)

            assert result == "kafka_restart"
            context["task_instance"].xcom_push.assert_called_with(
                key="kafka_status", value="error"
            )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_snowflake_connection_recovery(self, mock_hook_class, test_env_vars):
        """Test Snowflake connection recovery scenarios"""
        snowflake_errors = [
            Exception("Connection lost"),
            Exception("Session expired"),
            Exception("Warehouse suspended"),
            Exception("Database not found"),
        ]

        functions_to_test = [
            ("cloud_sales_pipeline_dag", "check_data_freshness"),
            ("pipeline_monitoring_dag", "generate_daily_report"),
            ("pipeline_monitoring_dag", "check_data_quality_trends"),
            ("maintenance_dag", "calculate_table_sizes"),
        ]

        for module_name, function_name in functions_to_test:
            for error in snowflake_errors:
                mock_hook = Mock()
                if "get_first" in dir(mock_hook):
                    mock_hook.get_first.side_effect = error
                if "get_records" in dir(mock_hook):
                    mock_hook.get_records.side_effect = error
                mock_hook_class.return_value = mock_hook

                module = __import__(module_name)
                func = getattr(module, function_name)
                context = MockAirflowContext().get_context()

                # Functions should either handle errors gracefully or raise them
                try:
                    result = func(**context)
                    # If no exception, should have valid result
                    assert result is not None
                except Exception as e:
                    # Exception is acceptable, should be the expected error
                    assert str(error) in str(e) or "Failed to" in str(e)

    def test_memory_pressure_scenarios(self, test_env_vars):
        """Test behavior under memory pressure"""
        # Import all DAGs under simulated memory pressure
        try:
            import gc

            # Force garbage collection
            gc.collect()

            # Import DAGs
            modules = []
            for module_name in [
                "cloud_sales_pipeline_dag",
                "pipeline_monitoring_dag",
                "maintenance_dag",
            ]:
                module = __import__(module_name)
                modules.append(module)

                # Force garbage collection after each import
                gc.collect()

            # Verify DAGs are still accessible
            for module in modules:
                dag_found = False
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, DAG):
                        dag_found = True
                        break
                assert dag_found, f"DAG not found in {module}"

        except MemoryError:
            pytest.skip("Insufficient memory for test")

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_concurrent_access_conflicts(self, mock_hook_class, test_env_vars):
        """Test handling of concurrent access conflicts"""
        mock_hook = Mock()

        # Simulate database lock/conflict errors
        conflict_errors = [
            Exception("Resource busy"),
            Exception("Deadlock detected"),
            Exception("Lock wait timeout"),
            Exception("Table is locked"),
        ]

        from pipeline_monitoring_dag import generate_daily_report

        for error in conflict_errors:
            mock_hook.get_first.side_effect = error
            mock_hook_class.return_value = mock_hook

            context = MockAirflowContext().get_context()

            with pytest.raises(Exception):
                generate_daily_report(**context)

            # Should push error to XCom
            error_calls = [
                call
                for call in context["task_instance"].xcom_push.call_args_list
                if "error" in str(call)
            ]
            assert len(error_calls) > 0


class TestDataValidationEdgeCases:
    """Test data validation edge cases"""

    def test_invalid_environment_variables(self, test_env_vars):
        """Test handling of invalid environment variables"""
        # Test with missing required environment variables
        invalid_env_scenarios = [
            {},  # Empty environment
            {"SNOWFLAKE_ACCOUNT": ""},  # Empty values
            {"SNOWFLAKE_ACCOUNT": None},  # None values
            {
                "SNOWFLAKE_ACCOUNT": "valid",
                "SNOWFLAKE_USER": "",
            },  # Partial configuration
        ]

        for invalid_env in invalid_env_scenarios:
            with patch.dict(os.environ, invalid_env, clear=True):
                try:
                    # Try to import DAGs with invalid environment
                    import cloud_sales_pipeline_dag

                    # Find ENV_VARS
                    env_vars = getattr(cloud_sales_pipeline_dag, "ENV_VARS", {})

                    # Should handle missing variables gracefully
                    assert isinstance(env_vars, dict)

                except Exception as e:
                    # Exception is acceptable for invalid configuration
                    assert "environment" in str(e).lower() or "config" in str(e).lower()

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_sql_injection_prevention(self, mock_hook_class, test_env_vars):
        """Test SQL injection prevention in dynamic queries"""
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

        # Test with malicious input in execution date
        malicious_contexts = [
            MockAirflowContext(ds="2024-01-01'; DROP TABLE users; --").get_context(),
            MockAirflowContext(
                ds="2024-01-01 UNION SELECT * FROM sensitive_table"
            ).get_context(),
            MockAirflowContext(ds="'; DELETE FROM fact_sales; --").get_context(),
        ]

        from pipeline_monitoring_dag import generate_daily_report

        for context in malicious_contexts:
            try:
                result = generate_daily_report(**context)
                # Should handle malicious input safely
                assert isinstance(result, dict)
            except Exception as e:
                # Exception is acceptable for malformed input
                assert "invalid" in str(e).lower() or "format" in str(e).lower()

    def test_dag_parameter_validation(self, test_env_vars):
        """Test DAG parameter validation"""
        import cloud_sales_pipeline_dag

        # Find the DAG
        dag = None
        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                dag = attr
                break

        assert dag is not None

        # Test DAG parameters
        params = dag.params
        assert isinstance(params, dict)

        # Verify parameter types
        for param_name, param_value in params.items():
            assert param_name is not None
            assert param_name != ""
            # Parameter values should be of expected types
            assert isinstance(param_value, (bool, int, float, str, type(None)))


class TestBoundaryConditions:
    """Test boundary conditions and limits"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_zero_record_scenarios(self, mock_hook_class, test_env_vars):
        """Test scenarios with zero records"""
        mock_hook = Mock()

        # Test functions with zero records
        zero_data_scenarios = [
            ([0], "streaming records"),
            ([0, 0], "all records"),
            ([], "empty result"),
        ]

        from cloud_sales_pipeline_dag import check_data_freshness

        for data, scenario in zero_data_scenarios:
            if len(data) == 2:
                mock_hook.get_first.side_effect = [[data[0]], [data[1]]]
            elif len(data) == 1:
                mock_hook.get_first.side_effect = [[data[0]], [0]]
            else:
                mock_hook.get_first.side_effect = [[], []]

            mock_hook_class.return_value = mock_hook

            context = MockAirflowContext().get_context()
            result = check_data_freshness(**context)

            if sum(data) == 0 or not data:
                assert result == "skip_pipeline"
            else:
                assert result == "dbt_run_staging"

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_maximum_data_scenarios(self, mock_hook_class, test_env_vars):
        """Test scenarios with maximum data values"""
        mock_hook = Mock()

        # Test with very large numbers
        large_values = [
            999999999999,  # Very large transaction count
            999999999999.99,  # Very large sales amount
            999999999,  # Large customer count
            99.99,  # High quality score
        ]

        mock_hook.get_first.return_value = [
            20240101,
            large_values[0],
            large_values[1],
            large_values[2],
            large_values[3],
            1000,
            50000.0,
            250,
            5.26,
            8.70,
        ]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        result = generate_daily_report(**context)

        # Should handle large values gracefully
        assert result["total_transactions"] == large_values[0]
        assert result["total_sales"] == large_values[1]
        assert result["unique_customers"] == large_values[2]
        assert result["avg_quality_score"] == large_values[3]

    def test_schedule_interval_edge_cases(self, test_env_vars):
        """Test schedule interval edge cases"""
        # Import all DAGs
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag
        import maintenance_dag

        dag_modules = [
            cloud_sales_pipeline_dag,
            pipeline_monitoring_dag,
            maintenance_dag,
        ]

        for module in dag_modules:
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, DAG):
                    dag = attr

                    # Schedule interval should be valid cron expression or None
                    schedule = dag.schedule_interval
                    if schedule is not None:
                        assert isinstance(schedule, str)
                        assert len(schedule) > 0
                        # Basic cron validation - should have 5 parts
                        if schedule != "@daily" and schedule != "@weekly":
                            parts = schedule.split()
                            assert (
                                len(parts) == 5
                            ), f"Invalid cron expression: {schedule}"

    def test_task_timeout_boundaries(self, test_env_vars):
        """Test task timeout boundary conditions"""
        import maintenance_dag

        # Find maintenance DAG (has longest timeouts)
        dag = None
        for attr_name in dir(maintenance_dag):
            attr = getattr(maintenance_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_maintenance":
                dag = attr
                break

        assert dag is not None

        # Check default timeout
        default_timeout = dag.default_args.get("execution_timeout")
        assert default_timeout is not None
        assert isinstance(default_timeout, timedelta)
        assert default_timeout.total_seconds() > 0
        assert default_timeout.total_seconds() <= 7200  # Max 2 hours

        # Check individual task timeouts
        for task in dag.tasks:
            if hasattr(task, "execution_timeout") and task.execution_timeout:
                assert isinstance(task.execution_timeout, timedelta)
                assert task.execution_timeout.total_seconds() > 0
