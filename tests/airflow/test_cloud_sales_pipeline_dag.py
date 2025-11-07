"""
Unit tests for cloud_sales_pipeline_dag.py
Tests DAG structure, task configuration, and business logic
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
from tests.airflow.test_utils import (
    assert_dag_structure,
    assert_task_dependencies,
    MockAirflowContext,
    MockSnowflakeHook,
    MockExternalServices,
)


@pytest.fixture
def cloud_sales_dag(test_env_vars):
    """Import and return the cloud sales pipeline DAG"""
    # Import the DAG module
    import cloud_sales_pipeline_dag

    # Find the DAG object
    for attr_name in dir(cloud_sales_pipeline_dag):
        attr = getattr(cloud_sales_pipeline_dag, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == "cloud_sales_pipeline":
            return attr

    pytest.fail("Could not find cloud_sales_pipeline DAG")


class TestCloudSalesPipelineDAGStructure:
    """Test DAG structure, configuration, and task definitions"""

    def test_dag_properties(self, cloud_sales_dag):
        """Test basic DAG properties"""
        assert cloud_sales_dag.dag_id == "cloud_sales_pipeline"
        assert (
            cloud_sales_dag.description
            == "Modern cloud sales data pipeline with Kafka, Spark, dbt, and Snowflake"
        )
        assert cloud_sales_dag.schedule_interval == "0 2 * * *"  # 2 AM daily
        assert cloud_sales_dag.catchup is False
        assert cloud_sales_dag.max_active_runs == 1

        # Check tags
        expected_tags = ["cloud", "sales", "kafka", "spark", "dbt", "snowflake"]
        assert set(cloud_sales_dag.tags) == set(expected_tags)

    def test_dag_default_args(self, cloud_sales_dag):
        """Test DAG default arguments"""
        default_args = cloud_sales_dag.default_args

        assert default_args["depends_on_past"] is False
        assert default_args["email_on_failure"] is True
        assert default_args["email_on_retry"] is False
        assert default_args["retries"] == 2
        assert default_args["retry_delay"] == timedelta(minutes=5)
        assert default_args["execution_timeout"] == timedelta(hours=2)
        assert default_args["sla"] == timedelta(hours=3)

    def test_dag_tasks_exist(self, cloud_sales_dag):
        """Test that all expected tasks exist in the DAG"""
        expected_tasks = [
            "start_pipeline",
            "kafka_health_check",
            "kafka_restart",
            "spark_batch_processing",
            "spark_batch_processing_docker",
            "data_freshness_check",
            "skip_pipeline",
            "dbt_deps",
            "dbt_run_staging",
            "dbt_run_intermediate",
            "dbt_run_marts",
            "dbt_test",
            "data_quality_monitor",
            "dbt_docs_generate",
            "success_notification",
            "failure_notification",
            "end_pipeline",
        ]

        assert_dag_structure(cloud_sales_dag, expected_tasks)

    def test_task_dependencies(self, cloud_sales_dag):
        """Test task dependencies are correctly defined"""
        # Test start pipeline dependencies
        assert_task_dependencies(
            cloud_sales_dag, "kafka_health_check", ["start_pipeline"]
        )

        # Test branching after Kafka health check
        kafka_health_task = cloud_sales_dag.get_task("kafka_health_check")
        downstream_tasks = {t.task_id for t in kafka_health_task.downstream_list}
        assert downstream_tasks == {"kafka_restart", "spark_batch_processing"}

        # Test Spark processing dependencies
        assert_task_dependencies(cloud_sales_dag, "spark_batch_processing", [])
        assert_task_dependencies(
            cloud_sales_dag, "data_freshness_check", ["spark_batch_processing"]
        )

        # Test dbt chain dependencies
        assert_task_dependencies(cloud_sales_dag, "dbt_run_staging", ["dbt_deps"])
        assert_task_dependencies(
            cloud_sales_dag, "dbt_run_intermediate", ["dbt_run_staging"]
        )
        assert_task_dependencies(
            cloud_sales_dag, "dbt_run_marts", ["dbt_run_intermediate"]
        )

        # Test final steps dependencies
        assert_task_dependencies(
            cloud_sales_dag, "dbt_docs_generate", ["dbt_test", "data_quality_monitor"]
        )
        assert_task_dependencies(
            cloud_sales_dag, "success_notification", ["dbt_docs_generate"]
        )


class TestKafkaHealthCheck:
    """Test Kafka health check functionality"""

    @patch("subprocess.run")
    def test_kafka_health_check_healthy(self, mock_subprocess, test_env_vars):
        """Test Kafka health check when Kafka is healthy"""
        # Mock successful subprocess call
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
        mock_subprocess.return_value = mock_result

        # Import the function
        from cloud_sales_pipeline_dag import check_kafka_health

        # Create mock context
        context = MockAirflowContext().get_context()

        # Execute function
        result = check_kafka_health(**context)

        # Assertions
        assert result == "spark_batch_processing"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="healthy"
        )
        mock_subprocess.assert_called_once()

    @patch("subprocess.run")
    def test_kafka_health_check_unhealthy(self, mock_subprocess, test_env_vars):
        """Test Kafka health check when Kafka is unhealthy"""
        # Mock subprocess call with unhealthy response
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = '{"connector":{"state":"FAILED"}}'
        mock_subprocess.return_value = mock_result

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="unhealthy"
        )

    @patch("subprocess.run")
    def test_kafka_health_check_connection_error(self, mock_subprocess, test_env_vars):
        """Test Kafka health check when connection fails"""
        # Mock failed subprocess call
        mock_result = Mock()
        mock_result.returncode = 1
        mock_subprocess.return_value = mock_result

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="error"
        )

    @patch("subprocess.run")
    def test_kafka_health_check_exception(self, mock_subprocess, test_env_vars):
        """Test Kafka health check when exception occurs"""
        # Mock subprocess to raise exception
        mock_subprocess.side_effect = Exception("Connection timeout")

        from cloud_sales_pipeline_dag import check_kafka_health

        context = MockAirflowContext().get_context()

        result = check_kafka_health(**context)

        assert result == "kafka_restart"
        context["task_instance"].xcom_push.assert_called_with(
            key="kafka_status", value="error"
        )


class TestDataFreshnessCheck:
    """Test data freshness check functionality"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_data_freshness_check_with_data(self, mock_hook_class, test_env_vars):
        """Test data freshness check when fresh data is available"""
        # Mock Snowflake hook to return data counts
        mock_hook = Mock()
        mock_hook.get_first.side_effect = [
            [1500],  # streaming_count
            [800],  # batch_count
        ]
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        result = check_data_freshness(**context)

        assert result == "dbt_run_staging"

        # Verify XCom pushes
        expected_calls = [
            call(key="streaming_records", value=1500),
            call(key="batch_records", value=800),
            call(key="total_records", value=2300),
        ]
        context["task_instance"].xcom_push.assert_has_calls(
            expected_calls, any_order=True
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_data_freshness_check_no_data(self, mock_hook_class, test_env_vars):
        """Test data freshness check when no fresh data is available"""
        mock_hook = Mock()
        mock_hook.get_first.side_effect = [[0], [0]]  # streaming_count  # batch_count
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        result = check_data_freshness(**context)

        assert result == "skip_pipeline"

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_data_freshness_check_exception(self, mock_hook_class, test_env_vars):
        """Test data freshness check when exception occurs"""
        mock_hook = Mock()
        mock_hook.get_first.side_effect = Exception("Database connection error")
        mock_hook_class.return_value = mock_hook

        from cloud_sales_pipeline_dag import check_data_freshness

        context = MockAirflowContext().get_context()

        result = check_data_freshness(**context)

        # Should proceed anyway in case of error
        assert result == "dbt_run_staging"


class TestTaskConfiguration:
    """Test individual task configurations"""

    def test_docker_operator_tasks(self, cloud_sales_dag, test_env_vars):
        """Test Docker operator tasks are configured correctly"""
        docker_tasks = [
            "kafka_restart",
            "spark_batch_processing_docker",
            "dbt_deps",
            "dbt_run_staging",
            "dbt_run_intermediate",
            "dbt_run_marts",
            "dbt_test",
            "dbt_docs_generate",
        ]

        for task_id in docker_tasks:
            task = cloud_sales_dag.get_task(task_id)
            assert task.auto_remove is True
            assert task.docker_url == "unix://var/run/docker.sock"
            assert (
                test_env_vars["PIPELINE_IMAGE"] in task.image
                or "sales-pipeline:latest" in task.image
            )

    def test_databricks_operator_configuration(self, cloud_sales_dag):
        """Test Databricks operator configuration"""
        task = cloud_sales_dag.get_task("spark_batch_processing")

        assert task.databricks_conn_id == "databricks_default"
        assert task.job_id == "{{ var.value.databricks_sales_etl_job_id }}"
        assert task.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

        # Check notebook parameters
        expected_params = ["input_path", "output_table", "batch_date"]
        for param in expected_params:
            assert param in task.notebook_params

    def test_snowflake_operator_configuration(self, cloud_sales_dag):
        """Test Snowflake operator configuration"""
        task = cloud_sales_dag.get_task("data_quality_monitor")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database == "SALES_DW" or task.database == "TEST_SALES_DW"
        assert task.schema == "MARTS"
        assert task.warehouse == "COMPUTE_WH" or task.warehouse == "TEST_COMPUTE_WH"
        assert task.sql is not None and len(task.sql.strip()) > 0

    def test_slack_notification_tasks(self, cloud_sales_dag):
        """Test Slack notification task configuration"""
        success_task = cloud_sales_dag.get_task("success_notification")
        failure_task = cloud_sales_dag.get_task("failure_notification")

        assert success_task.http_conn_id == "slack_webhook"
        assert success_task.trigger_rule == TriggerRule.ALL_SUCCESS
        assert "Successfully" in success_task.message  # Success message

        assert failure_task.http_conn_id == "slack_webhook"
        assert failure_task.trigger_rule == TriggerRule.ONE_FAILED
        assert "Failed" in failure_task.message  # Failure message

    def test_trigger_rules(self, cloud_sales_dag):
        """Test trigger rules are set correctly"""
        # Test tasks with specific trigger rules
        trigger_rule_tests = {
            "kafka_restart": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "spark_batch_processing": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "spark_batch_processing_docker": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "skip_pipeline": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "dbt_deps": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "dbt_test": TriggerRule.ALL_SUCCESS,
            "dbt_docs_generate": TriggerRule.ALL_SUCCESS,
            "success_notification": TriggerRule.ALL_SUCCESS,
            "failure_notification": TriggerRule.ONE_FAILED,
            "end_pipeline": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        }

        for task_id, expected_rule in trigger_rule_tests.items():
            task = cloud_sales_dag.get_task(task_id)
            assert (
                task.trigger_rule == expected_rule
            ), f"Task {task_id} has incorrect trigger rule"


class TestDAGParameters:
    """Test DAG parameters and templating"""

    def test_dag_params(self, cloud_sales_dag):
        """Test DAG parameters are defined correctly"""
        expected_params = {
            "run_spark_job": True,
            "run_full_refresh": False,
            "skip_tests": False,
        }

        assert cloud_sales_dag.params == expected_params

    def test_templating_in_tasks(self, cloud_sales_dag):
        """Test Jinja templating in task configurations"""
        databricks_task = cloud_sales_dag.get_task("spark_batch_processing")

        # Check templated parameters
        assert "{{ ds }}" in str(databricks_task.notebook_params["input_path"])
        assert "{{ ds }}" in str(databricks_task.notebook_params["batch_date"])

        # Check Slack notification templating
        success_task = cloud_sales_dag.get_task("success_notification")
        assert "{{ ds }}" in success_task.message
        assert "{{ ti.xcom_pull(" in success_task.message


class TestDAGImport:
    """Test DAG import and Python syntax"""

    def test_dag_import_no_errors(self, test_env_vars):
        """Test that DAG can be imported without errors"""
        try:
            import cloud_sales_pipeline_dag

            assert hasattr(cloud_sales_pipeline_dag, "dag")
        except ImportError as e:
            pytest.fail(f"Failed to import cloud_sales_pipeline_dag: {e}")
        except SyntaxError as e:
            pytest.fail(f"Syntax error in cloud_sales_pipeline_dag: {e}")

    def test_environment_variables_usage(self, cloud_sales_dag):
        """Test that environment variables are used correctly in DAG definition"""
        # Test that ENV_VARS dictionary is populated
        import cloud_sales_pipeline_dag

        assert hasattr(cloud_sales_pipeline_dag, "ENV_VARS")
        env_vars = cloud_sales_pipeline_dag.ENV_VARS

        required_env_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            "SNOWFLAKE_KEY_PASSPHRASE",
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN",
        ]

        for var in required_env_vars:
            assert var in env_vars


class TestErrorHandling:
    """Test error handling and recovery mechanisms"""

    def test_retry_configuration(self, cloud_sales_dag):
        """Test retry configuration for tasks"""
        # Most tasks should inherit retry settings from default_args
        for task in cloud_sales_dag.tasks:
            # Check that tasks have reasonable retry settings
            if hasattr(task, "retries"):
                assert task.retries >= 1 or task.retries is None  # Use default

    def test_failure_handling_workflow(self, cloud_sales_dag):
        """Test failure handling workflow exists"""
        failure_task = cloud_sales_dag.get_task("failure_notification")

        # Failure notification should be triggered by key pipeline tasks
        upstream_task_ids = {t.task_id for t in failure_task.upstream_list}
        expected_upstream_tasks = {
            "kafka_health_check",
            "spark_batch_processing",
            "dbt_run_staging",
            "dbt_run_intermediate",
            "dbt_run_marts",
            "dbt_test",
        }

        # At least some critical tasks should trigger failure notification
        assert len(upstream_task_ids.intersection(expected_upstream_tasks)) > 0


class TestDataQualitySQL:
    """Test data quality SQL queries"""

    def test_data_quality_monitor_sql(self, cloud_sales_dag):
        """Test data quality monitoring SQL is valid"""
        task = cloud_sales_dag.get_task("data_quality_monitor")
        sql = task.sql

        # Basic SQL validation
        assert "SELECT" in sql.upper()
        assert "FACT_SALES" in sql.upper()
        assert "DATA_QUALITY_SCORE" in sql.upper()
        assert "UNION ALL" in sql.upper()

        # Check for specific quality metrics
        assert "record_count" in sql
        assert "avg_quality_score" in sql
        assert "min_quality_score" in sql
        assert "low_quality_records" in sql


@pytest.mark.integration
class TestDAGExecution:
    """Integration tests for DAG execution"""

    def test_dag_can_be_instantiated(self, test_env_vars, mock_external_services):
        """Test DAG can be instantiated and tasks can be retrieved"""
        import cloud_sales_pipeline_dag

        dag = None
        for attr_name in dir(cloud_sales_pipeline_dag):
            attr = getattr(cloud_sales_pipeline_dag, attr_name)
            if isinstance(attr, DAG):
                dag = attr
                break

        assert dag is not None
        assert len(dag.tasks) > 0
        assert dag.dag_id == "cloud_sales_pipeline"

    def test_branching_logic_execution(self, test_env_vars):
        """Test that branching tasks can execute without errors"""
        from cloud_sales_pipeline_dag import check_kafka_health, check_data_freshness

        context = MockAirflowContext().get_context()

        # Test both functions can be called
        with patch("subprocess.run") as mock_subprocess:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
            mock_subprocess.return_value = mock_result

            kafka_result = check_kafka_health(**context)
            assert kafka_result in ["spark_batch_processing", "kafka_restart"]

        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_first.side_effect = [[100], [50]]

            freshness_result = check_data_freshness(**context)
            assert freshness_result in ["dbt_run_staging", "skip_pipeline"]
