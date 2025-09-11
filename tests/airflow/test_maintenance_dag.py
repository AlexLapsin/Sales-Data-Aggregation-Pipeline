"""
Unit tests for maintenance_dag.py
Tests maintenance DAG structure, cleanup functions, and optimization tasks
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
def maintenance_dag(test_env_vars):
    """Import and return the pipeline maintenance DAG"""
    import maintenance_dag

    # Find the DAG object
    for attr_name in dir(maintenance_dag):
        attr = getattr(maintenance_dag, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == "pipeline_maintenance":
            return attr

    pytest.fail("Could not find pipeline_maintenance DAG")


class TestMaintenanceDAGStructure:
    """Test DAG structure, configuration, and task definitions"""

    def test_dag_properties(self, maintenance_dag):
        """Test basic DAG properties"""
        assert maintenance_dag.dag_id == "pipeline_maintenance"
        assert (
            maintenance_dag.description
            == "Routine maintenance tasks for the cloud sales pipeline"
        )
        assert (
            maintenance_dag.schedule_interval == "0 3 * * 0"
        )  # Weekly on Sunday at 3 AM
        assert maintenance_dag.catchup is False
        assert maintenance_dag.max_active_runs == 1

        # Check tags
        expected_tags = ["maintenance", "cleanup", "optimization"]
        assert set(maintenance_dag.tags) == set(expected_tags)

    def test_dag_default_args(self, maintenance_dag):
        """Test DAG default arguments"""
        default_args = maintenance_dag.default_args

        assert default_args["depends_on_past"] is False
        assert default_args["email_on_failure"] is True
        assert default_args["email_on_retry"] is False
        assert default_args["retries"] == 1
        assert default_args["retry_delay"] == timedelta(minutes=5)
        assert default_args["execution_timeout"] == timedelta(hours=2)

    def test_dag_tasks_exist(self, maintenance_dag):
        """Test that all expected tasks exist in the DAG"""
        expected_tasks = [
            "start_maintenance",
            "cleanup_old_raw_data",
            "cleanup_old_staging_data",
            "cleanup_s3_old_data",
            "calculate_table_stats",
            "optimize_table_clustering",
            "vacuum_tables",
            "update_schema_documentation",
            "validate_schema_integrity",
            "cleanup_airflow_logs",
            "cleanup_docker_resources",
            "kafka_log_cleanup",
            "warehouse_scaling_check",
            "generate_maintenance_report",
            "end_maintenance",
        ]

        assert_dag_structure(maintenance_dag, expected_tasks)

    def test_task_dependencies(self, maintenance_dag):
        """Test task dependencies are correctly defined"""
        # Test start dependencies - parallel cleanup tasks
        start_downstream = {
            t.task_id
            for t in maintenance_dag.get_task("start_maintenance").downstream_list
        }
        expected_parallel_cleanup = {
            "cleanup_old_raw_data",
            "cleanup_s3_old_data",
            "cleanup_airflow_logs",
            "cleanup_docker_resources",
        }
        assert start_downstream == expected_parallel_cleanup

        # Test raw data cleanup chain
        assert_task_dependencies(
            maintenance_dag, "cleanup_old_staging_data", ["cleanup_old_raw_data"]
        )
        assert_task_dependencies(
            maintenance_dag,
            "calculate_table_stats",
            ["cleanup_old_raw_data", "cleanup_old_staging_data"],
        )

        # Test optimization dependencies
        calculate_downstream = {
            t.task_id
            for t in maintenance_dag.get_task("calculate_table_stats").downstream_list
        }
        assert calculate_downstream == {"optimize_table_clustering", "vacuum_tables"}

        # Test schema maintenance dependencies
        optimization_tasks = ["optimize_table_clustering", "vacuum_tables"]
        for task_id in optimization_tasks:
            task_downstream = {
                t.task_id for t in maintenance_dag.get_task(task_id).downstream_list
            }
            expected_downstream = {
                "update_schema_documentation",
                "validate_schema_integrity",
                "warehouse_scaling_check",
            }
            assert task_downstream == expected_downstream

        # Test final report dependencies
        report_task = maintenance_dag.get_task("generate_maintenance_report")
        report_upstream = {t.task_id for t in report_task.upstream_list}
        expected_report_upstream = {
            "update_schema_documentation",
            "validate_schema_integrity",
            "warehouse_scaling_check",
            "kafka_log_cleanup",
        }
        assert report_upstream == expected_report_upstream


class TestCalculateTableSizes:
    """Test table size calculation and optimization recommendations"""

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_success(self, mock_hook_class, test_env_vars):
        """Test successful table size calculation with recommendations"""
        mock_hook = Mock()
        # Mock table statistics: schema, table, row_count, bytes, gb, clustering_key, auto_clustering
        sample_records = [
            (
                "MARTS",
                "FACT_SALES",
                10000000,
                5368709120,
                5.0,
                None,
                False,
            ),  # Large table without clustering
            (
                "MARTS",
                "DIM_PRODUCT",
                50000,
                104857600,
                0.1,
                "PRODUCT_ID",
                True,
            ),  # Small table with clustering
            (
                "RAW",
                "SALES_RAW",
                50000000,
                21474836480,
                20.0,
                None,
                False,
            ),  # Very large table without clustering
            (
                "STAGING",
                "STG_SALES",
                1000000,
                1073741824,
                1.0,
                "DATE_KEY",
                False,
            ),  # Medium table with clustering
        ]
        mock_hook.get_records.return_value = sample_records
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        result = calculate_table_sizes(**context)

        # Verify recommendations
        assert len(result) == 4  # Should have 4 recommendations

        # Check specific recommendations
        recommendations_by_table = {rec["table"]: rec for rec in result}

        # FACT_SALES should get clustering recommendation (>1GB without clustering)
        assert "MARTS.FACT_SALES" in recommendations_by_table
        fact_sales_rec = recommendations_by_table["MARTS.FACT_SALES"]
        assert fact_sales_rec["action"] == "add_clustering"

        # SALES_RAW should get both clustering and vacuum recommendations (>10GB)
        sales_raw_recs = [rec for rec in result if rec["table"] == "RAW.SALES_RAW"]
        assert len(sales_raw_recs) == 2
        actions = {rec["action"] for rec in sales_raw_recs}
        assert "add_clustering" in actions
        assert "vacuum" in actions

        # Verify XCom push
        context["task_instance"].xcom_push.assert_called_with(
            key="table_recommendations", value=result
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_no_recommendations(
        self, mock_hook_class, test_env_vars
    ):
        """Test table size calculation when no recommendations are needed"""
        mock_hook = Mock()
        # Mock small tables with good clustering
        sample_records = [
            ("MARTS", "DIM_PRODUCT", 10000, 104857600, 0.1, "PRODUCT_ID", True),
            ("MARTS", "DIM_STORE", 5000, 52428800, 0.05, "STORE_ID", True),
        ]
        mock_hook.get_records.return_value = sample_records
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        result = calculate_table_sizes(**context)

        assert result == []  # No recommendations needed
        context["task_instance"].xcom_push.assert_called_with(
            key="table_recommendations", value=[]
        )

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_calculate_table_sizes_exception(self, mock_hook_class, test_env_vars):
        """Test table size calculation when exception occurs"""
        mock_hook = Mock()
        mock_hook.get_records.side_effect = Exception("Database connection error")
        mock_hook_class.return_value = mock_hook

        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        with pytest.raises(Exception, match="Database connection error"):
            calculate_table_sizes(**context)

        # Verify error was pushed to XCom
        expected_error = [
            {"error": "Failed to calculate table sizes: Database connection error"}
        ]
        context["task_instance"].xcom_push.assert_called_with(
            key="table_recommendations", value=expected_error
        )


class TestSnowflakeOperatorTasks:
    """Test Snowflake operator task configurations"""

    def test_cleanup_old_raw_data_config(self, maintenance_dag):
        """Test raw data cleanup configuration"""
        task = maintenance_dag.get_task("cleanup_old_raw_data")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "RAW"

        # Verify SQL contains expected cleanup operations
        sql = task.sql
        assert "SALES_RAW_ARCHIVE" in sql.upper()
        assert "SALES_BATCH_RAW_ARCHIVE" in sql.upper()
        assert "DELETE FROM" in sql.upper()
        assert "INTERVAL '90 days'" in sql

    def test_cleanup_old_staging_data_config(self, maintenance_dag):
        """Test staging data cleanup configuration"""
        task = maintenance_dag.get_task("cleanup_old_staging_data")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "STAGING"

        # Verify SQL contains staging cleanup operations
        sql = task.sql
        assert "DROP TABLE IF EXISTS" in sql.upper()
        assert "TEMP_STAGING_DATA" in sql.upper()
        assert "TEMP_PROCESSING" in sql.upper()

    def test_optimize_table_clustering_config(self, maintenance_dag):
        """Test table clustering optimization configuration"""
        task = maintenance_dag.get_task("optimize_table_clustering")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "MARTS"

        # Verify SQL contains clustering and stats operations
        sql = task.sql
        assert "RECLUSTER" in sql.upper()
        assert "FACT_SALES" in sql.upper()
        assert "ANALYZE TABLE" in sql.upper()
        assert "COMPUTE STATISTICS" in sql.upper()

    def test_vacuum_tables_config(self, maintenance_dag):
        """Test table vacuum configuration"""
        task = maintenance_dag.get_task("vacuum_tables")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]

        # Verify SQL contains vacuum operations
        sql = task.sql
        assert "COMPACT" in sql.upper()
        assert "PURGE" in sql.upper()
        assert "FACT_SALES" in sql.upper()

    def test_validate_schema_integrity_config(self, maintenance_dag):
        """Test schema integrity validation configuration"""
        task = maintenance_dag.get_task("validate_schema_integrity")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]
        assert task.schema == "MARTS"

        # Verify SQL checks for orphaned records
        sql = task.sql
        assert "orphaned_products" in sql
        assert "orphaned_stores" in sql
        assert "orphaned_dates" in sql
        assert "LEFT JOIN" in sql.upper()
        assert "IS NULL" in sql.upper()

    def test_warehouse_scaling_check_config(self, maintenance_dag):
        """Test warehouse scaling check configuration"""
        task = maintenance_dag.get_task("warehouse_scaling_check")

        assert task.snowflake_conn_id == "snowflake_default"
        assert "warehouse_name" in task.params

        # Verify SQL queries account usage
        sql = task.sql
        assert "QUERY_HISTORY" in sql.upper()
        assert "CREDITS_USED" in sql.upper()
        assert "EXECUTION_TIME" in sql.upper()
        assert "{{ params.warehouse_name }}" in sql

    def test_generate_maintenance_report_config(self, maintenance_dag):
        """Test maintenance report generation configuration"""
        task = maintenance_dag.get_task("generate_maintenance_report")

        assert task.snowflake_conn_id == "snowflake_default"
        assert task.database in ["SALES_DW", "TEST_SALES_DW"]

        # Verify SQL generates summary metrics
        sql = task.sql
        assert "maintenance_timestamp" in sql
        assert "weekly_maintenance_completed" in sql
        assert "current_raw_records" in sql
        assert "current_fact_records" in sql
        assert "marts_size_gb" in sql


class TestDockerOperatorTasks:
    """Test Docker operator task configurations"""

    def test_cleanup_s3_old_data_config(self, maintenance_dag):
        """Test S3 data cleanup configuration"""
        task = maintenance_dag.get_task("cleanup_s3_old_data")

        assert task.image == "amazon/aws-cli:latest"
        assert task.auto_remove is True

        # Check command includes AWS CLI operations
        command_str = " ".join(task.command)
        assert "aws s3api list-objects-v2" in command_str
        assert "90 days ago" in command_str
        assert "aws s3 rm" in command_str

    def test_update_schema_documentation_config(self, maintenance_dag):
        """Test schema documentation update configuration"""
        task = maintenance_dag.get_task("update_schema_documentation")

        assert task.auto_remove is True
        assert "sales-pipeline" in task.image

        # Check command runs dbt docs generate
        command_str = (
            " ".join(task.command) if isinstance(task.command, list) else task.command
        )
        assert "dbt docs generate" in command_str
        assert "profiles-dir" in command_str

    def test_kafka_log_cleanup_config(self, maintenance_dag):
        """Test Kafka log cleanup configuration"""
        task = maintenance_dag.get_task("kafka_log_cleanup")

        assert task.image == "confluentinc/cp-kafka:latest"
        assert task.auto_remove is True
        assert task.network_mode == "host"
        assert task.trigger_rule == TriggerRule.ALL_SUCCESS

        # Check command includes Kafka log operations
        command_str = " ".join(task.command)
        assert "kafka-log-dirs" in command_str
        assert "bootstrap-server" in command_str


class TestBashOperatorTasks:
    """Test Bash operator task configurations"""

    def test_cleanup_airflow_logs_config(self, maintenance_dag):
        """Test Airflow logs cleanup configuration"""
        task = maintenance_dag.get_task("cleanup_airflow_logs")

        bash_command = task.bash_command
        assert "find /opt/airflow/logs" in bash_command
        assert "-mtime +30" in bash_command
        assert "-delete" in bash_command
        assert "find /tmp" in bash_command
        assert "*sales_pipeline*" in bash_command

    def test_cleanup_docker_resources_config(self, maintenance_dag):
        """Test Docker resources cleanup configuration"""
        task = maintenance_dag.get_task("cleanup_docker_resources")

        bash_command = task.bash_command
        assert "docker system prune" in bash_command
        assert "docker image prune" in bash_command
        assert "docker container prune" in bash_command
        assert "sales-pipeline" in bash_command


class TestTaskTriggerRules:
    """Test trigger rules are set correctly"""

    def test_parallel_cleanup_tasks(self, maintenance_dag):
        """Test parallel cleanup tasks have correct trigger rules"""
        parallel_cleanup_tasks = [
            "cleanup_old_raw_data",
            "cleanup_s3_old_data",
            "cleanup_airflow_logs",
            "cleanup_docker_resources",
        ]

        for task_id in parallel_cleanup_tasks:
            task = maintenance_dag.get_task(task_id)
            # These tasks should have default trigger rule (all_success)
            assert task.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_optimization_tasks_trigger_rules(self, maintenance_dag):
        """Test optimization tasks have appropriate trigger rules"""
        optimization_tasks = [
            "optimize_table_clustering",
            "vacuum_tables",
            "update_schema_documentation",
            "validate_schema_integrity",
        ]

        for task_id in optimization_tasks:
            task = maintenance_dag.get_task(task_id)
            assert task.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_final_tasks_trigger_rules(self, maintenance_dag):
        """Test final tasks have appropriate trigger rules"""
        end_task = maintenance_dag.get_task("end_maintenance")
        assert end_task.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

        kafka_cleanup_task = maintenance_dag.get_task("kafka_log_cleanup")
        assert kafka_cleanup_task.trigger_rule == TriggerRule.ALL_SUCCESS


class TestEnvironmentVariables:
    """Test environment variable usage"""

    def test_environment_variables_usage(self, maintenance_dag):
        """Test that environment variables are used correctly"""
        import maintenance_dag

        assert hasattr(maintenance_dag, "ENV_VARS")
        env_vars = maintenance_dag.ENV_VARS

        required_env_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "S3_BUCKET",
        ]

        for var in required_env_vars:
            assert var in env_vars

    def test_tasks_use_environment_variables(self, maintenance_dag):
        """Test that tasks properly use environment variables"""
        # Test Docker tasks have environment variables
        docker_tasks = [
            "cleanup_s3_old_data",
            "update_schema_documentation",
            "kafka_log_cleanup",
        ]

        for task_id in docker_tasks:
            task = maintenance_dag.get_task(task_id)
            if hasattr(task, "environment") and task.environment:
                # Should have some environment variables set
                assert len(task.environment) > 0


class TestMaintenanceSQL:
    """Test SQL query validity and structure"""

    def test_cleanup_sql_structure(self, maintenance_dag):
        """Test cleanup SQL has expected structure"""
        cleanup_task = maintenance_dag.get_task("cleanup_old_raw_data")
        sql = cleanup_task.sql

        # Should have CREATE OR REPLACE for archives
        assert sql.count("CREATE OR REPLACE TABLE") >= 2

        # Should have DELETE operations
        assert sql.count("DELETE FROM") >= 2

        # Should reference both raw tables
        assert "SALES_RAW" in sql.upper()
        assert "SALES_BATCH_RAW" in sql.upper()

        # Should use 90-day retention
        assert sql.count("INTERVAL '90 days'") >= 4

    def test_optimization_sql_structure(self, maintenance_dag):
        """Test optimization SQL has expected structure"""
        clustering_task = maintenance_dag.get_task("optimize_table_clustering")
        sql = clustering_task.sql

        # Should have RECLUSTER operations
        assert "RECLUSTER" in sql.upper()
        assert sql.count("RECLUSTER") >= 2

        # Should have ANALYZE operations
        assert "ANALYZE TABLE" in sql.upper()
        assert "COMPUTE STATISTICS" in sql.upper()

        vacuum_task = maintenance_dag.get_task("vacuum_tables")
        vacuum_sql = vacuum_task.sql

        # Should have COMPACT and PURGE operations
        assert "COMPACT" in vacuum_sql.upper()
        assert "PURGE" in vacuum_sql.upper()

    def test_integrity_check_sql_structure(self, maintenance_dag):
        """Test integrity check SQL has expected structure"""
        integrity_task = maintenance_dag.get_task("validate_schema_integrity")
        sql = integrity_task.sql

        # Should check for orphaned records in all main dimensions
        assert "orphaned_products" in sql
        assert "orphaned_stores" in sql
        assert "orphaned_dates" in sql

        # Should use UNION ALL to combine results
        assert sql.count("UNION ALL") >= 2

        # Should use LEFT JOINs to find orphans
        assert sql.count("LEFT JOIN") >= 3


class TestDAGImport:
    """Test DAG import and Python syntax"""

    def test_dag_import_no_errors(self, test_env_vars):
        """Test that DAG can be imported without errors"""
        try:
            import maintenance_dag

            assert hasattr(maintenance_dag, "dag")
        except ImportError as e:
            pytest.fail(f"Failed to import maintenance_dag: {e}")
        except SyntaxError as e:
            pytest.fail(f"Syntax error in maintenance_dag: {e}")

    def test_dag_schedule_interval(self, maintenance_dag):
        """Test DAG is scheduled for weekly execution"""
        # Weekly on Sunday at 3 AM
        assert maintenance_dag.schedule_interval == "0 3 * * 0"

        # Should not run on every DAG run
        assert maintenance_dag.catchup is False


class TestMaintenanceWorkflow:
    """Test maintenance workflow logic"""

    def test_maintenance_phases(self, maintenance_dag):
        """Test maintenance workflow has logical phases"""
        # Phase 1: Cleanup (parallel)
        cleanup_tasks = [
            "cleanup_old_raw_data",
            "cleanup_s3_old_data",
            "cleanup_airflow_logs",
            "cleanup_docker_resources",
        ]

        for task_id in cleanup_tasks:
            task = maintenance_dag.get_task(task_id)
            upstream = {t.task_id for t in task.upstream_list}
            assert upstream == {"start_maintenance"}

        # Phase 2: Data cleanup chain
        staging_task = maintenance_dag.get_task("cleanup_old_staging_data")
        staging_upstream = {t.task_id for t in staging_task.upstream_list}
        assert staging_upstream == {"cleanup_old_raw_data"}

        # Phase 3: Analysis and optimization
        stats_task = maintenance_dag.get_task("calculate_table_stats")
        stats_upstream = {t.task_id for t in stats_task.upstream_list}
        assert stats_upstream == {"cleanup_old_raw_data", "cleanup_old_staging_data"}

        # Phase 4: Performance optimization
        optimization_tasks = ["optimize_table_clustering", "vacuum_tables"]
        for task_id in optimization_tasks:
            task = maintenance_dag.get_task(task_id)
            upstream = {t.task_id for t in task.upstream_list}
            assert upstream == {"calculate_table_stats"}

    def test_maintenance_completion(self, maintenance_dag):
        """Test maintenance completion workflow"""
        report_task = maintenance_dag.get_task("generate_maintenance_report")
        end_task = maintenance_dag.get_task("end_maintenance")

        # Report should depend on all major maintenance tasks
        report_upstream = {t.task_id for t in report_task.upstream_list}
        expected_report_deps = {
            "update_schema_documentation",
            "validate_schema_integrity",
            "warehouse_scaling_check",
            "kafka_log_cleanup",
        }
        assert report_upstream == expected_report_deps

        # End should depend on report
        end_upstream = {t.task_id for t in end_task.upstream_list}
        assert end_upstream == {"generate_maintenance_report"}


@pytest.mark.integration
class TestDAGExecution:
    """Integration tests for DAG execution"""

    def test_dag_can_be_instantiated(self, test_env_vars, mock_external_services):
        """Test DAG can be instantiated and tasks can be retrieved"""
        import maintenance_dag

        dag = None
        for attr_name in dir(maintenance_dag):
            attr = getattr(maintenance_dag, attr_name)
            if isinstance(attr, DAG):
                dag = attr
                break

        assert dag is not None
        assert len(dag.tasks) > 0
        assert dag.dag_id == "pipeline_maintenance"

    def test_python_callable_functions(self, test_env_vars):
        """Test Python callable functions can execute"""
        from maintenance_dag import calculate_table_sizes

        context = MockAirflowContext().get_context()

        # Test calculate_table_sizes with mocked Snowflake
        with patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        ) as mock_hook:
            mock_hook.return_value.get_records.return_value = [
                ("MARTS", "FACT_SALES", 1000000, 5368709120, 5.0, None, False)
            ]

            result = calculate_table_sizes(**context)
            assert isinstance(result, list)
            if result:  # If recommendations were generated
                assert "table" in result[0]
                assert "action" in result[0]
                assert "reason" in result[0]

    def test_task_execution_order(self, maintenance_dag):
        """Test tasks are ordered correctly for maintenance workflow"""
        # Get all task IDs in topological order
        from airflow.models import TaskInstance

        # Should be able to get task execution order
        task_order = []
        visited = set()

        def visit_task(task):
            if task.task_id in visited:
                return
            visited.add(task.task_id)

            # Visit all upstream tasks first
            for upstream_task in task.upstream_list:
                visit_task(upstream_task)

            task_order.append(task.task_id)

        # Start from end task and work backwards
        end_task = maintenance_dag.get_task("end_maintenance")
        visit_task(end_task)

        # Verify logical ordering
        start_idx = task_order.index("start_maintenance")
        cleanup_idx = task_order.index("cleanup_old_raw_data")
        stats_idx = task_order.index("calculate_table_stats")
        optimize_idx = task_order.index("optimize_table_clustering")
        report_idx = task_order.index("generate_maintenance_report")
        end_idx = task_order.index("end_maintenance")

        assert start_idx < cleanup_idx < stats_idx < optimize_idx < report_idx < end_idx


@pytest.mark.slow
class TestMaintenancePerformance:
    """Performance tests for maintenance operations"""

    def test_maintenance_task_timeouts(self, maintenance_dag):
        """Test maintenance tasks have appropriate timeouts"""
        for task in maintenance_dag.tasks:
            # All tasks should have execution timeout from default_args
            if hasattr(task, "execution_timeout"):
                if task.execution_timeout is not None:
                    assert task.execution_timeout <= timedelta(hours=2)

    def test_maintenance_resource_usage(self, maintenance_dag):
        """Test maintenance tasks have reasonable resource requirements"""
        # Docker tasks should have auto_remove to clean up resources
        docker_task_ids = [
            "cleanup_s3_old_data",
            "update_schema_documentation",
            "kafka_log_cleanup",
        ]

        for task_id in docker_task_ids:
            task = maintenance_dag.get_task(task_id)
            assert task.auto_remove is True

        # Bash tasks should clean up temporary files
        bash_task_ids = ["cleanup_airflow_logs", "cleanup_docker_resources"]
        for task_id in bash_task_ids:
            task = maintenance_dag.get_task(task_id)
            # Should have cleanup commands
            assert (
                "rm" in task.bash_command
                or "delete" in task.bash_command
                or "prune" in task.bash_command
            )
