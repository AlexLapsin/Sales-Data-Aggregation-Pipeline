"""
Unit tests for pipeline_monitoring_dag.py
Tests monitoring DAG structure, reporting functions, and alert configurations
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
)


@pytest.fixture
def monitoring_dag(test_env_vars):
    """Import and return the pipeline monitoring DAG"""
    import pipeline_monitoring_dag

    # Find the DAG object
    for attr_name in dir(pipeline_monitoring_dag):
        attr = getattr(pipeline_monitoring_dag, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == "pipeline_monitoring":
            return attr

    pytest.fail("Could not find pipeline_monitoring DAG")


class TestPipelineMonitoringDAGStructure:
    """Test DAG structure, configuration, and task definitions"""

    def test_dag_properties(self, monitoring_dag):
        """Test basic DAG properties"""
        assert monitoring_dag.dag_id == "pipeline_monitoring"
        assert (
            monitoring_dag.description
            == "Monitors pipeline health, performance, and data quality"
        )
        assert monitoring_dag.schedule_interval == "0 8 * * *"  # 8 AM daily
        assert monitoring_dag.catchup is False
        assert monitoring_dag.max_active_runs == 1

        # Check tags
        expected_tags = ["monitoring", "data-quality", "alerts"]
        assert set(monitoring_dag.tags) == set(expected_tags)

    def test_dag_default_args(self, monitoring_dag):
        """Test DAG default arguments"""
        default_args = monitoring_dag.default_args

        assert default_args["depends_on_past"] is False
        assert default_args["email_on_failure"] is True
        assert default_args["email_on_retry"] is False
        assert default_args["retries"] == 1
        assert default_args["retry_delay"] == timedelta(minutes=2)
        assert default_args["execution_timeout"] == timedelta(minutes=30)

    def test_dag_tasks_exist(self, monitoring_dag):
        """Test that all expected tasks exist in the DAG"""
        expected_tasks = [
            "start_monitoring",
            "check_streaming_freshness",
            "check_batch_freshness",
            "pipeline_performance_check",
            "generate_daily_report",
            "check_quality_trends",
            "kafka_cluster_health",
            "snowflake_warehouse_health",
            "daily_report_email",
            "quality_alert_slack",
            "end_monitoring",
        ]

        assert_dag_structure(monitoring_dag, expected_tasks)

    def test_task_dependencies(self, monitoring_dag):
        """Test task dependencies are correctly defined"""
        # Test start dependencies
        start_downstream = {
            t.task_id
            for t in monitoring_dag.get_task("start_monitoring").downstream_list
        }
        expected_parallel_tasks = {
            "check_streaming_freshness",
            "check_batch_freshness",
            "pipeline_performance_check",
            "kafka_cluster_health",
            "snowflake_warehouse_health",
        }
        assert start_downstream == expected_parallel_tasks

        # Test report generation dependencies
        assert_task_dependencies(
            monitoring_dag,
            "generate_daily_report",
            [
                "check_streaming_freshness",
                "check_batch_freshness",
                "pipeline_performance_check",
            ],
        )

        # Test quality trends dependencies
        assert_task_dependencies(
            monitoring_dag, "check_quality_trends", ["generate_daily_report"]
        )

        # Test final notification dependencies
        email_task = monitoring_dag.get_task("daily_report_email")
        email_upstream = {t.task_id for t in email_task.upstream_list}
        expected_upstream = {
            "generate_daily_report",
            "check_quality_trends",
            "kafka_cluster_health",
            "snowflake_warehouse_health",
        }
        assert email_upstream == expected_upstream


class TestGenerateDailyReport:
    """Test daily report generation functionality"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_generate_daily_report_success(self, mock_hook_class, test_env_vars):
        """Test successful daily report generation"""
        # Mock Snowflake hook to return sample data
        mock_hook = Mock()
        sample_result = [
            20240101,  # date_key
            1000,  # total_transactions
            50000.0,  # total_sales
            250,  # unique_customers
            85.5,  # avg_quality_score
            950,  # prev_transactions
            46000.0,  # prev_sales
            240,  # prev_customers
            5.26,  # transaction_growth
            8.70,  # sales_growth
        ]
        mock_hook.get_first.return_value = sample_result
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext(ds="2024-01-01").get_context()

        result = generate_daily_report(**context)

        # Verify result structure
        assert result["date"] == "2024-01-01"
        assert result["total_transactions"] == 1000
        assert result["total_sales"] == 50000.0
        assert result["unique_customers"] == 250
        assert result["avg_quality_score"] == 85.5
        assert result["transaction_growth"] == 5.26
        assert result["sales_growth"] == 8.70

        # Verify XCom push
        context["task_instance"].xcom_push.assert_called_with(
            key="daily_report", value=result
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_generate_daily_report_no_data(self, mock_hook_class, test_env_vars):
        """Test daily report generation when no data is available"""
        mock_hook = Mock()
        mock_hook.get_first.return_value = None
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        result = generate_daily_report(**context)

        assert "error" in result
        assert result["error"] == "No data available for yesterday"
        context["task_instance"].xcom_push.assert_called_with(
            key="daily_report", value=result
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_generate_daily_report_exception(self, mock_hook_class, test_env_vars):
        """Test daily report generation when exception occurs"""
        mock_hook = Mock()
        mock_hook.get_first.side_effect = Exception("Database connection error")
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import generate_daily_report

        context = MockAirflowContext().get_context()

        with pytest.raises(Exception, match="Database connection error"):
            generate_daily_report(**context)

        # Verify error was pushed to XCom
        expected_error = {
            "error": "Failed to generate daily report: Database connection error"
        }
        context["task_instance"].xcom_push.assert_called_with(
            key="daily_report", value=expected_error
        )


class TestDataQualityTrends:
    """Test data quality trend analysis functionality"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_check_quality_trends_no_issues(self, mock_hook_class, test_env_vars):
        """Test quality trends check when no issues are found"""
        mock_hook = Mock()
        # Mock good quality data (no issues)
        sample_records = [
            (datetime(2024, 1, 1), 87.5, 1000, 20, 2.0),  # Good quality
            (datetime(2024, 1, 2), 89.2, 1100, 30, 2.7),  # Good quality
            (datetime(2024, 1, 3), 86.8, 950, 25, 2.6),  # Good quality
        ]
        mock_hook.get_records.return_value = sample_records
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        result = check_data_quality_trends(**context)

        assert result == []  # No issues found
        context["task_instance"].xcom_push.assert_called_with(
            key="quality_issues", value=[]
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_check_quality_trends_with_issues(self, mock_hook_class, test_env_vars):
        """Test quality trends check when issues are found"""
        mock_hook = Mock()
        # Mock data with quality issues
        sample_records = [
            (
                datetime(2024, 1, 1),
                82.0,
                1000,
                150,
                15.0,
            ),  # Low avg quality + high low-quality %
            (datetime(2024, 1, 2), 89.2, 1100, 30, 2.7),  # Good quality
            (datetime(2024, 1, 3), 83.5, 950, 120, 12.6),  # High low-quality %
        ]
        mock_hook.get_records.return_value = sample_records
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        result = check_data_quality_trends(**context)

        # Should find 3 issues: low avg quality on 2024-01-01, high % on 2024-01-01 and 2024-01-03
        assert len(result) == 3

        # Check specific issues
        issues_by_date = {issue["date"]: issue for issue in result}

        assert "2024-01-01" in issues_by_date
        assert "2024-01-03" in issues_by_date

        # Verify XCom push
        context["task_instance"].xcom_push.assert_called_with(
            key="quality_issues", value=result
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_check_quality_trends_exception(self, mock_hook_class, test_env_vars):
        """Test quality trends check when exception occurs"""
        mock_hook = Mock()
        mock_hook.get_records.side_effect = Exception("Query execution failed")
        mock_hook_class.return_value = mock_hook

        from pipeline_monitoring_dag import check_data_quality_trends

        context = MockAirflowContext().get_context()

        with pytest.raises(Exception, match="Query execution failed"):
            check_data_quality_trends(**context)

        # Verify error was pushed to XCom
        expected_error = [
            {"error": "Failed to check quality trends: Query execution failed"}
        ]
        context["task_instance"].xcom_push.assert_called_with(
            key="quality_issues", value=expected_error
        )


class TestSnowflakeOperatorTasks:
    """Test Snowflake operator task configurations"""

    def test_streaming_freshness_check_config(self, monitoring_dag):
        """Test streaming data freshness check configuration"""
        task = monitoring_dag.get_task("check_streaming_freshness")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "RAW"

        # Verify SQL contains expected elements
        sql = task.sql
        assert "streaming_data_freshness" in sql
        assert "SALES_RAW" in sql.upper()
        assert "INGESTION_TIMESTAMP" in sql.upper()
        assert "DATEDIFF" in sql.upper()

    def test_batch_freshness_check_config(self, monitoring_dag):
        """Test batch data freshness check configuration"""
        task = monitoring_dag.get_task("check_batch_freshness")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "RAW"

        # Verify SQL contains expected elements
        sql = task.sql
        assert "batch_data_freshness" in sql
        assert "SALES_BATCH_RAW" in sql.upper()
        assert "BATCH_ID" in sql.upper()

    def test_performance_check_config(self, monitoring_dag):
        """Test pipeline performance check configuration"""
        task = monitoring_dag.get_task("pipeline_performance_check")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "MARTS"

        # Verify SQL queries Snowflake system tables
        sql = task.sql
        assert "QUERY_HISTORY" in sql.upper()
        assert "EXECUTION_TIME" in sql.upper()
        assert "dbt" in sql

    def test_warehouse_health_check_config(self, monitoring_dag):
        """Test Snowflake warehouse health check configuration"""
        task = monitoring_dag.get_task("snowflake_warehouse_health")

        assert task.snowflake_conn_id == "snowflake_default"
        assert "warehouse_name" in task.params

        # Verify SQL queries system information
        sql = task.sql
        assert "INFORMATION_SCHEMA.WAREHOUSES" in sql.upper()
        assert "WAREHOUSE_NAME" in sql.upper()
        assert "{{ params.warehouse_name }}" in sql


class TestDockerOperatorTasks:
    """Test Docker operator task configurations"""

    def test_kafka_cluster_health_config(self, monitoring_dag):
        """Test Kafka cluster health check configuration"""
        task = monitoring_dag.get_task("kafka_cluster_health")

        assert task.image == "confluentinc/cp-kafka:latest"
        assert task.auto_remove is True
        assert task.network_mode == "host"

        # Check command includes Kafka tools
        command_str = " ".join(task.command)
        assert "kafka-topics" in command_str
        assert "kafka-consumer-groups" in command_str
        assert "bootstrap-server" in command_str


class TestEmailOperator:
    """Test email notification configuration"""

    def test_daily_report_email_config(self, monitoring_dag):
        """Test daily report email configuration"""
        task = monitoring_dag.get_task("daily_report_email")

        assert task.subject == "Daily Sales Pipeline Report - {{ ds }}"
        assert task.trigger_rule == TriggerRule.ALL_SUCCESS

        # Check HTML content includes expected sections
        html_content = task.html_content
        assert "Daily Sales Pipeline Report" in html_content
        assert "Daily Metrics" in html_content
        assert "Growth Metrics" in html_content
        assert "Quality Issues" in html_content

        # Check for Jinja templating
        assert "{{ ds }}" in html_content
        assert "{{ ti.xcom_pull(" in html_content

    def test_email_template_variables(self, monitoring_dag):
        """Test email template uses correct XCom variables"""
        task = monitoring_dag.get_task("daily_report_email")
        html_content = task.html_content

        # Check for specific XCom pulls
        expected_xcom_calls = [
            "ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')",
            "ti.xcom_pull(key='quality_issues', task_ids='check_quality_trends')",
        ]

        for xcom_call in expected_xcom_calls:
            assert xcom_call in html_content


class TestSlackOperator:
    """Test Slack notification configuration"""

    def test_quality_alert_slack_config(self, monitoring_dag):
        """Test Slack quality alert configuration"""
        task = monitoring_dag.get_task("quality_alert_slack")

        assert task.http_conn_id == "slack_webhook"
        assert task.trigger_rule == TriggerRule.ALL_SUCCESS

        # Check message content
        message = task.message
        assert "Data Quality Alert" in message
        assert "{{ ds }}" in message
        assert "ti.xcom_pull(key='quality_issues'" in message
        assert "quality_issues|length > 0" in message


class TestTaskTriggerRules:
    """Test trigger rules are set correctly"""

    def test_parallel_execution_tasks(self, monitoring_dag):
        """Test tasks that should run in parallel have correct trigger rules"""
        parallel_tasks = [
            "check_streaming_freshness",
            "check_batch_freshness",
            "pipeline_performance_check",
            "kafka_cluster_health",
            "snowflake_warehouse_health",
        ]

        for task_id in parallel_tasks:
            task = monitoring_dag.get_task(task_id)
            # These tasks should have default trigger rule (all_success)
            assert task.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_final_tasks_trigger_rules(self, monitoring_dag):
        """Test final tasks have appropriate trigger rules"""
        # Email and Slack should succeed even if some checks fail
        email_task = monitoring_dag.get_task("daily_report_email")
        slack_task = monitoring_dag.get_task("quality_alert_slack")
        end_task = monitoring_dag.get_task("end_monitoring")

        assert email_task.trigger_rule == TriggerRule.ALL_SUCCESS
        assert slack_task.trigger_rule == TriggerRule.ALL_SUCCESS
        assert end_task.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


class TestDAGImport:
    """Test DAG import and Python syntax"""

    def test_dag_import_no_errors(self, test_env_vars):
        """Test that DAG can be imported without errors"""
        try:
            import pipeline_monitoring_dag

            assert hasattr(pipeline_monitoring_dag, "dag")
        except ImportError as e:
            pytest.fail(f"Failed to import pipeline_monitoring_dag: {e}")
        except SyntaxError as e:
            pytest.fail(f"Syntax error in pipeline_monitoring_dag: {e}")

    def test_environment_variables_usage(self, monitoring_dag):
        """Test that environment variables are used correctly"""
        import pipeline_monitoring_dag

        assert hasattr(pipeline_monitoring_dag, "ENV_VARS")
        env_vars = pipeline_monitoring_dag.ENV_VARS

        required_env_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "KAFKA_BOOTSTRAP_SERVERS",
        ]

        for var in required_env_vars:
            assert var in env_vars


class TestSQLQueries:
    """Test SQL query validity and structure"""

    def test_daily_metrics_query_structure(self, test_env_vars):
        """Test daily metrics query has expected structure"""
        from pipeline_monitoring_dag import generate_daily_report

        # Extract the query from the function (this is a bit tricky, but we can test the logic)
        # The SQL should query daily stats and previous day stats
        import inspect

        source = inspect.getsource(generate_daily_report)

        # Basic checks for SQL structure
        assert "daily_metrics_query" in source
        assert "daily_stats" in source
        assert "prev_day_stats" in source
        assert "FACT_SALES_DAILY" in source
        assert "DATE_KEY" in source

    def test_quality_trend_query_structure(self, test_env_vars):
        """Test quality trend query has expected structure"""
        from pipeline_monitoring_dag import check_data_quality_trends

        import inspect

        source = inspect.getsource(check_data_quality_trends)

        # Basic checks for SQL structure
        assert "quality_trend_query" in source
        assert "FACT_SALES" in source
        assert "DIM_DATE" in source
        assert "DATA_QUALITY_SCORE" in source
        assert "INTERVAL '7 days'" in source


@pytest.mark.integration
class TestDAGExecution:
    """Integration tests for DAG execution"""

    def test_dag_can_be_instantiated(self, test_env_vars, mock_external_services):
        """Test DAG can be instantiated and tasks can be retrieved"""
        import pipeline_monitoring_dag

        dag = None
        for attr_name in dir(pipeline_monitoring_dag):
            attr = getattr(pipeline_monitoring_dag, attr_name)
            if isinstance(attr, DAG):
                dag = attr
                break

        assert dag is not None
        assert len(dag.tasks) > 0
        assert dag.dag_id == "pipeline_monitoring"

    def test_python_callable_functions(self, test_env_vars):
        """Test Python callable functions can execute"""
        from pipeline_monitoring_dag import (
            generate_daily_report,
            check_data_quality_trends,
        )

        context = MockAirflowContext().get_context()

        # Test generate_daily_report with mocked Snowflake
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

            result = generate_daily_report(**context)
            assert isinstance(result, dict)
            assert "date" in result

        # Test check_data_quality_trends with mocked Snowflake
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_records.return_value = [
                (datetime(2024, 1, 1), 85.0, 1000, 50, 5.0)
            ]

            result = check_data_quality_trends(**context)
            assert isinstance(result, list)
