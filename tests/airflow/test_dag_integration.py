"""
Integration tests for cross-DAG dependencies and interactions
Tests the complete pipeline orchestration across all three DAGs
"""

import pytest
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, call
import json

# Add the DAG directory to Python path
dag_dir = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
sys.path.insert(0, dag_dir)

from airflow import DAG
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun, TaskInstance
from tests.airflow.test_utils import (
    MockAirflowContext,
    MockSnowflakeHook,
    create_mock_dag_run,
    create_mock_task_instance,
)


class TestDAGScheduling:
    """Test DAG scheduling and timing relationships"""

    def test_dag_schedule_intervals(self, test_env_vars):
        """Test that DAGs are scheduled at appropriate times"""
        # Import all DAGs
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag
        import maintenance_dag

        # Find DAG objects
        cloud_dag = None
        monitoring_dag = None
        maintenance_dag_obj = None

        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                cloud_dag = attr
                break

        for attr_name in dir(pipeline_monitoring_dag):
            attr = getattr(pipeline_monitoring_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_monitoring":
                monitoring_dag = attr
                break

        for attr_name in dir(maintenance_dag):
            attr = getattr(maintenance_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_maintenance":
                maintenance_dag_obj = attr
                break

        assert cloud_dag is not None
        assert monitoring_dag is not None
        assert maintenance_dag_obj is not None

        # Test schedule intervals
        assert cloud_dag.schedule_interval == "0 2 * * *"  # 2 AM daily
        assert (
            monitoring_dag.schedule_interval == "0 8 * * *"
        )  # 8 AM daily (after main pipeline)
        assert (
            maintenance_dag_obj.schedule_interval == "0 3 * * 0"
        )  # 3 AM Sunday weekly

        # Test timing logic - monitoring runs after main pipeline
        from datetime import time

        pipeline_time = time(2, 0)  # 2 AM
        monitoring_time = time(8, 0)  # 8 AM

        assert (
            monitoring_time > pipeline_time
        ), "Monitoring should run after main pipeline"

    def test_dag_execution_dependencies(self, test_env_vars):
        """Test logical dependencies between DAGs"""
        # Main pipeline should complete before monitoring starts
        # This is ensured by scheduling times, not explicit dependencies

        # Test that monitoring DAG checks for pipeline completion data
        import pipeline_monitoring_dag

        # The monitoring DAG should query tables populated by the main pipeline
        monitoring_functions = [
            pipeline_monitoring_dag.generate_daily_report,
            pipeline_monitoring_dag.check_data_quality_trends,
        ]

        for func in monitoring_functions:
            import inspect

            source = inspect.getsource(func)
            # Should reference tables created by main pipeline
            assert any(
                table in source.upper()
                for table in [
                    "FACT_SALES",
                    "FACT_SALES_DAILY",
                    "SALES_RAW",
                    "SALES_BATCH_RAW",
                ]
            )


class TestCrossDagDataFlow:
    """Test data flow and dependencies between DAGs"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_pipeline_to_monitoring_data_flow(self, mock_hook_class, test_env_vars):
        """Test data flows correctly from pipeline to monitoring"""
        # Mock Snowflake to simulate data created by pipeline
        mock_hook = Mock()

        # Simulate data that would be created by cloud_sales_pipeline
        pipeline_data = [
            1000,  # streaming_records
            500,  # batch_records
        ]

        monitoring_data = [
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

        # Test data freshness check from pipeline DAG
        mock_hook.get_first.side_effect = pipeline_data
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        result = check_data_freshness(**context)
        assert result == "dbt_run_staging"  # Should proceed with fresh data

        # Test monitoring DAG can read pipeline results
        mock_hook.get_first.side_effect = [monitoring_data]
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        monitoring_result = generate_daily_report(**context)

        assert monitoring_result["total_transactions"] == 1000
        assert monitoring_result["total_sales"] == 50000.0

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_monitoring_to_maintenance_coordination(
        self, mock_hook_class, test_env_vars
    ):
        """Test coordination between monitoring and maintenance"""
        mock_hook = Mock()

        # Simulate table statistics that would trigger maintenance actions
        table_stats = [
            (
                "MARTS",
                "FACT_SALES",
                10000000,
                21474836480,
                20.0,
                None,
                False,
            ),  # Large table needing optimization
            (
                "RAW",
                "SALES_RAW",
                50000000,
                53687091200,
                50.0,
                None,
                False,
            ),  # Very large table
        ]

        mock_hook.get_records.return_value = table_stats
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        recommendations = calculate_table_sizes(**context)

        # Should recommend clustering and vacuum for large tables
        assert len(recommendations) > 0
        actions = {rec["action"] for rec in recommendations}
        assert "add_clustering" in actions
        assert "vacuum" in actions


class TestErrorHandlingAcrossDAGs:
    """Test error handling and recovery across DAGs"""

    def test_pipeline_failure_monitoring_continues(self, test_env_vars):
        """Test that monitoring continues even if pipeline fails"""
        # Import DAGs
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag

        # Find DAG objects
        monitoring_dag = None
        for attr_name in dir(pipeline_monitoring_dag):
            attr = getattr(pipeline_monitoring_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_monitoring":
                monitoring_dag = attr
                break

        assert monitoring_dag is not None

        # Monitoring DAG should have tasks that can handle missing data
        # Check that generate_daily_report handles no data gracefully
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_first.return_value = None  # No data

            from pipeline_monitoring_dag import generate_daily_report

            context = MockAirflowContext().get_context()

            result = generate_daily_report(**context)
            assert "error" in result
            assert "No data available" in result["error"]

    def test_maintenance_handles_missing_tables(self, test_env_vars):
        """Test that maintenance handles missing or empty tables gracefully"""
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_records.return_value = []  # No tables

            from maintenance_dag import calculate_table_sizes

            context = MockAirflowContext().get_context()

            result = calculate_table_sizes(**context)
            assert result == []  # No recommendations for no tables


class TestNotificationCoordination:
    """Test notification coordination across DAGs"""

    def test_notification_channels_consistency(self, test_env_vars):
        """Test that all DAGs use consistent notification channels"""
        # Import all DAGs
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag

        cloud_dag = None
        monitoring_dag = None

        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                cloud_dag = attr
                break

        for attr_name in dir(pipeline_monitoring_dag):
            attr = getattr(pipeline_monitoring_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_monitoring":
                monitoring_dag = attr
                break

        # Check Slack notifications use same connection
        cloud_slack_tasks = [
            task for task in cloud_dag.tasks if "slack" in task.task_id.lower()
        ]
        monitoring_slack_tasks = [
            task for task in monitoring_dag.tasks if "slack" in task.task_id.lower()
        ]

        for task in cloud_slack_tasks + monitoring_slack_tasks:
            assert task.http_conn_id == "slack_webhook"

        # Check email notifications use consistent format
        monitoring_email_tasks = [
            task for task in monitoring_dag.tasks if "email" in task.task_id.lower()
        ]

        for task in monitoring_email_tasks:
            assert hasattr(task, "subject")
            assert "{{ ds }}" in task.subject  # Should use execution date

    def test_alert_escalation_logic(self, test_env_vars):
        """Test alert escalation across DAGs"""
        # Pipeline failures should trigger immediate alerts
        import cloud_sales_pipeline_dag

        cloud_dag = None
        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                cloud_dag = attr
                break

        failure_task = cloud_dag.get_task("failure_notification")

        # Should be triggered by critical pipeline tasks
        upstream_task_ids = {t.task_id for t in failure_task.upstream_list}
        critical_tasks = {
            "kafka_health_check",
            "spark_batch_processing",
            "dbt_run_staging",
            "dbt_run_intermediate",
            "dbt_run_marts",
        }

        # At least some critical tasks should trigger failure notification
        assert len(upstream_task_ids.intersection(critical_tasks)) > 0


class TestResourceManagement:
    """Test resource management across DAGs"""

    def test_snowflake_warehouse_usage(self, test_env_vars):
        """Test Snowflake warehouse usage across DAGs"""
        # Import all DAGs
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag
        import maintenance_dag

        # All DAGs should use the same warehouse configuration
        warehouses = set()

        # Check cloud pipeline DAG
        for attr_name in dir(cloud_sales_pipeline_dag):
            if attr_name == "ENV_VARS":
                env_vars = getattr(cloud_sales_pipeline_dag, attr_name)
                warehouses.add(env_vars.get("SNOWFLAKE_WAREHOUSE"))

        # Check monitoring DAG
        for attr_name in dir(pipeline_monitoring_dag):
            if attr_name == "ENV_VARS":
                env_vars = getattr(pipeline_monitoring_dag, attr_name)
                warehouses.add(env_vars.get("SNOWFLAKE_WAREHOUSE"))

        # Check maintenance DAG
        for attr_name in dir(maintenance_dag):
            if attr_name == "ENV_VARS":
                env_vars = getattr(maintenance_dag, attr_name)
                warehouses.add(env_vars.get("SNOWFLAKE_WAREHOUSE"))

        # Remove None values
        warehouses.discard(None)

        # Should all use same warehouse
        assert len(warehouses) <= 1, f"Multiple warehouses configured: {warehouses}"

    def test_docker_resource_coordination(self, test_env_vars):
        """Test Docker resource coordination across DAGs"""
        # Import all DAGs
        import cloud_sales_pipeline_dag
        import maintenance_dag

        cloud_dag = None
        maintenance_dag_obj = None

        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                cloud_dag = attr
                break

        for attr_name in dir(maintenance_dag):
            attr = getattr(maintenance_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "pipeline_maintenance":
                maintenance_dag_obj = attr
                break

        # Check Docker tasks use auto_remove for resource cleanup
        all_docker_tasks = []

        for task in cloud_dag.tasks:
            if hasattr(task, "image") and hasattr(task, "auto_remove"):
                all_docker_tasks.append(task)

        for task in maintenance_dag_obj.tasks:
            if hasattr(task, "image") and hasattr(task, "auto_remove"):
                all_docker_tasks.append(task)

        # All Docker tasks should have auto_remove enabled
        for task in all_docker_tasks:
            assert (
                task.auto_remove is True
            ), f"Task {task.task_id} should have auto_remove=True"


class TestDataQualityWorkflow:
    """Test end-to-end data quality workflow across DAGs"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_quality_monitoring_to_maintenance_feedback(
        self, mock_hook_class, test_env_vars
    ):
        """Test quality issues trigger appropriate maintenance actions"""
        mock_hook = Mock()

        # Simulate quality issues found by monitoring
        quality_issues_records = [
            (datetime(2024, 1, 1), 75.0, 1000, 300, 30.0),  # Very poor quality
            (datetime(2024, 1, 2), 82.0, 1100, 200, 18.2),  # Poor quality
        ]

        mock_hook.get_records.return_value = quality_issues_records
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        quality_issues = check_data_quality_trends(**context)

        # Should detect multiple quality issues
        assert len(quality_issues) >= 2

        # Simulate maintenance response to quality issues
        table_stats_for_maintenance = [
            (
                "MARTS",
                "FACT_SALES",
                10000000,
                21474836480,
                20.0,
                None,
                False,
            ),  # Large unclustered table
        ]

        mock_hook.get_records.return_value = table_stats_for_maintenance

        from maintenance_dag import calculate_table_sizes

        recommendations = calculate_table_sizes(**context)

        # Should recommend clustering to improve quality
        clustering_recs = [
            rec for rec in recommendations if rec["action"] == "add_clustering"
        ]
        assert len(clustering_recs) > 0


class TestXComDataFlow:
    """Test XCom data sharing across DAG functions"""

    def test_xcom_data_structure_consistency(self, test_env_vars):
        """Test XCom data structures are consistent across DAGs"""
        context = MockAirflowContext().get_context()

        # Test cloud pipeline XCom outputs
        with patch("subprocess.run") as mock_subprocess:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
            mock_subprocess.return_value = mock_result

            from cloud_sales_pipeline_dag import check_kafka_health

            kafka_result = check_kafka_health(**context)

            # Verify XCom was called with correct structure
            context["task_instance"].xcom_push.assert_called_with(
                key="kafka_status", value="healthy"
            )

        # Test monitoring DAG XCom outputs
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_first.return_value = [
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

            from pipeline_monitoring_dag import generate_daily_report

            report_result = generate_daily_report(**context)

            # Verify report structure
            required_keys = [
                "date",
                "total_transactions",
                "total_sales",
                "unique_customers",
                "avg_quality_score",
            ]
            for key in required_keys:
                assert key in report_result

    def test_xcom_error_handling(self, test_env_vars):
        """Test XCom error handling across DAGs"""
        context = MockAirflowContext().get_context()

        # Test error handling in monitoring functions
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_first.side_effect = Exception("Database error")

            from pipeline_monitoring_dag import generate_daily_report

            with pytest.raises(Exception):
                generate_daily_report(**context)

            # Should push error to XCom
            error_call = None
            for call_args in context["task_instance"].xcom_push.call_args_list:
                if "error" in str(call_args):
                    error_call = call_args
                    break

            assert error_call is not None


@pytest.mark.integration
class TestEndToEndWorkflow:
    """Test complete end-to-end workflow across all DAGs"""

    def test_complete_pipeline_simulation(self, test_env_vars):
        """Simulate complete pipeline execution across all DAGs"""
        execution_date = datetime(2024, 1, 1, 2, 0, 0)

        # Phase 1: Cloud Sales Pipeline
        with (
            patch("subprocess.run") as mock_subprocess,
            patch(
                "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
            ) as mock_snowflake,
        ):

            # Mock healthy Kafka
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
            mock_subprocess.return_value = mock_result

            # Mock fresh data available
            mock_snowflake.return_value.get_first.side_effect = [
                [1000],
                [500],
            ]  # streaming and batch data

            from cloud_sales_pipeline_dag import (
                check_kafka_health,
                check_data_freshness,
            )

            context = MockAirflowContext(ds="2024-01-01").get_context()

            # Execute pipeline checks
            kafka_status = check_kafka_health(**context)
            assert kafka_status == "spark_batch_processing"

            data_status = check_data_freshness(**context)
            assert data_status == "dbt_run_staging"

        # Phase 2: Pipeline Monitoring (6 hours later)
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_snowflake:
            # Mock data created by pipeline
            daily_metrics = [
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
            quality_trends = [(datetime(2024, 1, 1), 85.5, 1000, 50, 5.0)]

            mock_snowflake.return_value.get_first.return_value = daily_metrics
            mock_snowflake.return_value.get_records.return_value = quality_trends

            from pipeline_monitoring_dag import (
                generate_daily_report,
                check_data_quality_trends,
            )

            monitoring_context = MockAirflowContext(ds="2024-01-01").get_context()

            # Execute monitoring
            daily_report = generate_daily_report(**monitoring_context)
            quality_issues = check_data_quality_trends(**monitoring_context)

            assert daily_report["total_transactions"] == 1000
            assert len(quality_issues) == 0  # No quality issues

        # Phase 3: Maintenance (Weekly)
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_snowflake:
            # Mock table statistics
            table_stats = [
                ("MARTS", "FACT_SALES", 10000000, 5368709120, 5.0, None, False),
                ("RAW", "SALES_RAW", 50000000, 21474836480, 20.0, None, False),
            ]

            mock_snowflake.return_value.get_records.return_value = table_stats

            from maintenance_dag import calculate_table_sizes

            maintenance_context = MockAirflowContext(ds="2024-01-07").get_context()

            # Execute maintenance analysis
            recommendations = calculate_table_sizes(**maintenance_context)

            assert len(recommendations) > 0
            assert any(rec["action"] == "add_clustering" for rec in recommendations)

    def test_dag_import_all_together(self, test_env_vars):
        """Test all DAGs can be imported together without conflicts"""
        try:
            # Import all DAG modules
            import cloud_sales_pipeline_dag
            import pipeline_monitoring_dag
            import maintenance_dag

            # Verify each has its DAG
            cloud_dag = None
            monitoring_dag = None
            maintenance_dag_obj = None

            for attr_name in dir(cloud_sales_pipeline_dag):
                attr = getattr(cloud_sales_pipeline_dag, attr_name)
                if isinstance(attr, DAG):
                    cloud_dag = attr
                    break

            for attr_name in dir(pipeline_monitoring_dag):
                attr = getattr(pipeline_monitoring_dag, attr_name)
                if isinstance(attr, DAG):
                    monitoring_dag = attr
                    break

            for attr_name in dir(maintenance_dag):
                attr = getattr(maintenance_dag, attr_name)
                if isinstance(attr, DAG):
                    maintenance_dag_obj = attr
                    break

            assert cloud_dag is not None
            assert monitoring_dag is not None
            assert maintenance_dag_obj is not None

            # Verify unique DAG IDs
            dag_ids = {
                cloud_dag.dag_id,
                monitoring_dag.dag_id,
                maintenance_dag_obj.dag_id,
            }
            assert len(dag_ids) == 3, "DAG IDs should be unique"

        except Exception as e:
            pytest.fail(f"Failed to import all DAGs together: {e}")


@pytest.mark.external
class TestExternalServiceIntegration:
    """Test integration with external services across DAGs"""

    def test_snowflake_connection_consistency(self, test_env_vars):
        """Test Snowflake connections are consistent across DAGs"""
        # All DAGs should use the same Snowflake connection ID
        snowflake_tasks = []

        # Import all DAGs and collect Snowflake tasks
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag
        import maintenance_dag

        for module in [
            cloud_sales_pipeline_dag,
            pipeline_monitoring_dag,
            maintenance_dag,
        ]:
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, DAG):
                    dag = attr
                    for task in dag.tasks:
                        if hasattr(task, "snowflake_conn_id"):
                            snowflake_tasks.append(task)

        # All should use same connection ID
        conn_ids = {task.snowflake_conn_id for task in snowflake_tasks}
        assert len(conn_ids) == 1, f"Multiple Snowflake connection IDs: {conn_ids}"
        assert "snowflake_default" in conn_ids

    def test_databricks_integration(self, test_env_vars):
        """Test Databricks integration in cloud pipeline"""
        import cloud_sales_pipeline_dag

        cloud_dag = None
        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
                cloud_dag = attr
                break

        databricks_task = cloud_dag.get_task("spark_batch_processing")

        # Verify Databricks configuration
        assert databricks_task.databricks_conn_id == "databricks_default"
        assert databricks_task.job_id == "{{ var.value.databricks_sales_etl_job_id }}"

        # Should have proper notebook parameters
        assert "input_path" in databricks_task.notebook_params
        assert "output_table" in databricks_task.notebook_params
        assert "batch_date" in databricks_task.notebook_params

    def test_kafka_integration_across_dags(self, test_env_vars):
        """Test Kafka integration consistency"""
        # Import DAGs that use Kafka
        import cloud_sales_pipeline_dag
        import pipeline_monitoring_dag
        import maintenance_dag

        # Check environment variables for Kafka configuration
        kafka_configs = []

        for module in [
            cloud_sales_pipeline_dag,
            pipeline_monitoring_dag,
            maintenance_dag,
        ]:
            if hasattr(module, "ENV_VARS"):
                env_vars = getattr(module, "ENV_VARS")
                if "KAFKA_BOOTSTRAP_SERVERS" in env_vars:
                    kafka_configs.append(env_vars["KAFKA_BOOTSTRAP_SERVERS"])

        # Should all use same Kafka configuration
        unique_configs = set(kafka_configs)
        assert (
            len(unique_configs) <= 1
        ), f"Multiple Kafka configurations: {unique_configs}"
