"""
Test utilities for Airflow DAG testing
Provides common fixtures, mocks, and helper functions for DAG testing
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from unittest.mock import Mock, MagicMock, patch
import pytest
from airflow import DAG
from airflow.models import TaskInstance, DagRun
from airflow.utils.state import State
from airflow.utils.dates import days_ago


class MockAirflowContext:
    """Mock Airflow context for testing"""

    def __init__(self, ds: str = "2024-01-01", **kwargs):
        self.ds = ds
        self.execution_date = datetime.strptime(ds, "%Y-%m-%d")
        self.task_instance = Mock()
        self.dag_run = Mock()
        self.params = kwargs.get("params", {})

        # Mock task instance methods
        self.task_instance.xcom_push = Mock()
        self.task_instance.xcom_pull = Mock()
        self.task_instance.task_id = kwargs.get("task_id", "test_task")
        self.task_instance.dag_id = kwargs.get("dag_id", "test_dag")
        self.task_instance.start_date = self.execution_date
        self.task_instance.end_date = self.execution_date + timedelta(hours=1)

    def get_context(self) -> Dict[str, Any]:
        """Return context dictionary for task execution"""
        return {
            "ds": self.ds,
            "execution_date": self.execution_date,
            "task_instance": self.task_instance,
            "dag_run": self.dag_run,
            "params": self.params,
            "ti": self.task_instance,
        }


class MockedEnvironmentVariables:
    """Context manager for mocking environment variables"""

    def __init__(self, env_vars: Dict[str, str]):
        self.env_vars = env_vars
        self.original_values = {}

    def __enter__(self):
        for key, value in self.env_vars.items():
            self.original_values[key] = os.getenv(key)
            os.environ[key] = value
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key in self.env_vars:
            if self.original_values[key] is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = self.original_values[key]


def create_mock_dag_run(
    dag_id: str, execution_date: datetime, state: str = State.RUNNING
) -> Mock:
    """Create a mock DAG run for testing"""
    dag_run = Mock(spec=DagRun)
    dag_run.dag_id = dag_id
    dag_run.execution_date = execution_date
    dag_run.state = state
    dag_run.run_id = f"test_run_{execution_date.strftime('%Y%m%d_%H%M%S')}"
    return dag_run


def create_mock_task_instance(
    task_id: str, dag_id: str, execution_date: datetime, state: str = State.SUCCESS
) -> Mock:
    """Create a mock task instance for testing"""
    ti = Mock(spec=TaskInstance)
    ti.task_id = task_id
    ti.dag_id = dag_id
    ti.execution_date = execution_date
    ti.state = state
    ti.start_date = execution_date
    ti.end_date = execution_date + timedelta(minutes=5)
    ti.xcom_push = Mock()
    ti.xcom_pull = Mock()
    return ti


def get_test_env_vars() -> Dict[str, str]:
    """Get standard test environment variables"""
    return {
        "AWS_ACCESS_KEY_ID": "test_access_key",
        "AWS_SECRET_ACCESS_KEY": "test_secret_key",
        "AWS_DEFAULT_REGION": "us-east-1",
        "S3_BUCKET": "test-sales-bucket",
        "PROCESSED_BUCKET": "test-processed-bucket",
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_password",
        "SNOWFLAKE_DATABASE": "TEST_SALES_DW",
        "SNOWFLAKE_WAREHOUSE": "TEST_COMPUTE_WH",
        "SNOWFLAKE_SCHEMA": "TEST_RAW",
        "SNOWFLAKE_ROLE": "SYSADMIN",
        "DATABRICKS_HOST": "https://test.databricks.com",
        "DATABRICKS_TOKEN": "test_token",
        "DATABRICKS_CLUSTER_ID": "test_cluster_id",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_TOPIC": "test_sales_events",
        "PIPELINE_IMAGE": "test-sales-pipeline:latest",
        "OWNER_NAME": "test-data-engineering",
        "ALERT_EMAIL": "test@company.com",
    }


class MockSnowflakeHook:
    """Mock Snowflake hook for testing"""

    def __init__(self, results: Optional[List] = None, records: Optional[List] = None):
        self.results = results or []
        self.records = records or []
        self.executed_queries = []

    def get_first(self, sql: str) -> Optional[tuple]:
        """Mock get_first method"""
        self.executed_queries.append(sql)
        return self.results[0] if self.results else None

    def get_records(self, sql: str) -> List[tuple]:
        """Mock get_records method"""
        self.executed_queries.append(sql)
        return self.records

    def run(self, sql: str) -> None:
        """Mock run method"""
        self.executed_queries.append(sql)


class MockDockerOperator:
    """Mock Docker operator for testing"""

    def __init__(self, **kwargs):
        self.task_id = kwargs.get("task_id", "test_task")
        self.image = kwargs.get("image", "test:latest")
        self.command = kwargs.get("command", "")
        self.environment = kwargs.get("environment", {})
        self.auto_remove = kwargs.get("auto_remove", True)
        self.executed = False
        self.return_code = 0

    def execute(self, context):
        """Mock execute method"""
        self.executed = True
        if self.return_code != 0:
            raise Exception(
                f"Docker command failed with return code {self.return_code}"
            )
        return "Command executed successfully"


@pytest.fixture
def mock_airflow_context():
    """Pytest fixture for mock Airflow context"""
    return MockAirflowContext()


@pytest.fixture
def test_env_vars():
    """Pytest fixture for test environment variables"""
    env_vars = get_test_env_vars()
    with MockedEnvironmentVariables(env_vars):
        yield env_vars


@pytest.fixture
def mock_snowflake_hook():
    """Pytest fixture for mock Snowflake hook"""
    return MockSnowflakeHook()


def assert_dag_structure(dag: DAG, expected_tasks: List[str]):
    """Assert DAG has expected task structure"""
    actual_tasks = set(dag.task_ids)
    expected_tasks_set = set(expected_tasks)

    missing_tasks = expected_tasks_set - actual_tasks
    extra_tasks = actual_tasks - expected_tasks_set

    assert not missing_tasks, f"Missing expected tasks: {missing_tasks}"
    assert not extra_tasks, f"Unexpected extra tasks: {extra_tasks}"


def assert_task_dependencies(dag: DAG, task_id: str, upstream_task_ids: List[str]):
    """Assert task has expected upstream dependencies"""
    task = dag.get_task(task_id)
    actual_upstream = {t.task_id for t in task.upstream_list}
    expected_upstream = set(upstream_task_ids)

    assert actual_upstream == expected_upstream, (
        f"Task {task_id} upstream dependencies mismatch. "
        f"Expected: {expected_upstream}, Actual: {actual_upstream}"
    )


def assert_task_downstream(dag: DAG, task_id: str, downstream_task_ids: List[str]):
    """Assert task has expected downstream dependencies"""
    task = dag.get_task(task_id)
    actual_downstream = {t.task_id for t in task.downstream_list}
    expected_downstream = set(downstream_task_ids)

    assert actual_downstream == expected_downstream, (
        f"Task {task_id} downstream dependencies mismatch. "
        f"Expected: {expected_downstream}, Actual: {actual_downstream}"
    )


def mock_successful_subprocess_run(*args, **kwargs):
    """Mock subprocess.run for successful execution"""
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
    mock_result.stderr = ""
    return mock_result


def mock_failed_subprocess_run(*args, **kwargs):
    """Mock subprocess.run for failed execution"""
    mock_result = Mock()
    mock_result.returncode = 1
    mock_result.stdout = ""
    mock_result.stderr = "Connection failed"
    return mock_result


# Context managers for patching common external dependencies
class MockExternalServices:
    """Context manager for mocking external services"""

    def __init__(
        self, snowflake_results=None, kafka_healthy=True, databricks_success=True
    ):
        self.snowflake_results = snowflake_results or []
        self.kafka_healthy = kafka_healthy
        self.databricks_success = databricks_success
        self.patches = []

    def __enter__(self):
        # Mock Snowflake Hook
        snowflake_hook_patch = patch(
            "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook"
        )
        mock_hook = snowflake_hook_patch.start()
        mock_hook.return_value = MockSnowflakeHook(results=self.snowflake_results)
        self.patches.append(snowflake_hook_patch)

        # Mock subprocess for Kafka health checks
        subprocess_patch = patch("subprocess.run")
        mock_subprocess = subprocess_patch.start()
        if self.kafka_healthy:
            mock_subprocess.side_effect = mock_successful_subprocess_run
        else:
            mock_subprocess.side_effect = mock_failed_subprocess_run
        self.patches.append(subprocess_patch)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for patch_obj in self.patches:
            patch_obj.stop()


class MockDatabricksOperator:
    """Enhanced mock for Databricks operations"""

    def __init__(self, job_id=None, notebook_params=None, success=True, run_id=12345):
        self.job_id = job_id or "test_job_id"
        self.notebook_params = notebook_params or {}
        self.success = success
        self.run_id = run_id
        self.executed = False

    def execute(self, context):
        """Mock execute method"""
        self.executed = True
        if not self.success:
            raise Exception(f"Databricks job {self.job_id} failed")
        return {"run_id": self.run_id, "state": "SUCCESS"}


class MockSlackWebhookOperator:
    """Enhanced mock for Slack webhook operations"""

    def __init__(self, message=None, success=True):
        self.message = message or "Test message"
        self.success = success
        self.executed = False

    def execute(self, context):
        """Mock execute method"""
        self.executed = True
        if not self.success:
            raise Exception("Slack webhook failed")
        return "Message sent successfully"


class MockEmailOperator:
    """Enhanced mock for email operations"""

    def __init__(self, to=None, subject=None, html_content=None, success=True):
        self.to = to or ["test@example.com"]
        self.subject = subject or "Test Subject"
        self.html_content = html_content or "Test Content"
        self.success = success
        self.executed = False

    def execute(self, context):
        """Mock execute method"""
        self.executed = True
        if not self.success:
            raise Exception("Email sending failed")
        return "Email sent successfully"


class TestDataGenerator:
    """Utility class for generating test data"""

    @staticmethod
    def generate_daily_metrics(date_key=20240101, base_transactions=1000):
        """Generate realistic daily metrics data"""
        return [
            date_key,
            base_transactions,
            base_transactions * 50.0,  # Total sales
            base_transactions // 4,  # Unique customers
            85.5 + (date_key % 10),  # Quality score
            base_transactions - 50,  # Previous transactions
            (base_transactions - 50) * 48.0,  # Previous sales
            (base_transactions - 50) // 4,  # Previous customers
            5.26,  # Transaction growth
            8.70,  # Sales growth
        ]

    @staticmethod
    def generate_quality_trends(num_days=7, base_date=None):
        """Generate quality trend data for testing"""
        if base_date is None:
            base_date = datetime(2024, 1, 1)

        trends = []
        for i in range(num_days):
            date = base_date + timedelta(days=i)
            quality_score = 85.0 + (i % 20) - 10  # Vary between 75-95
            record_count = 1000 + (i * 100)
            low_quality_count = int(record_count * (100 - quality_score) / 100 * 0.1)
            low_quality_pct = (low_quality_count / record_count) * 100

            trends.append(
                (date, quality_score, record_count, low_quality_count, low_quality_pct)
            )

        return trends

    @staticmethod
    def generate_table_stats(num_tables=5, large_tables=2):
        """Generate table statistics for maintenance testing"""
        tables = []
        schemas = ["MARTS", "RAW", "STAGING"]

        for i in range(num_tables):
            schema = schemas[i % len(schemas)]
            table_name = f"TABLE_{i}"

            if i < large_tables:
                # Large table
                row_count = 10000000 + (i * 5000000)
                bytes_size = 21474836480 + (i * 10737418240)  # 20GB+
                gb_size = bytes_size / (1024**3)
                clustering_key = None if i % 2 == 0 else "CLUSTER_KEY"
            else:
                # Small table
                row_count = 100000 + (i * 10000)
                bytes_size = 104857600 + (i * 52428800)  # 100MB+
                gb_size = bytes_size / (1024**3)
                clustering_key = "CLUSTER_KEY" if i % 3 == 0 else None

            auto_clustering = clustering_key is not None and i % 4 == 0

            tables.append(
                (
                    schema,
                    table_name,
                    row_count,
                    bytes_size,
                    gb_size,
                    clustering_key,
                    auto_clustering,
                )
            )

        return tables


def validate_dag_import(dag_file_path: str) -> DAG:
    """
    Validate that a DAG file can be imported without errors
    Returns the DAG object if successful
    """
    # Add the DAG directory to Python path
    dag_dir = os.path.dirname(dag_file_path)
    if dag_dir not in sys.path:
        sys.path.insert(0, dag_dir)

    try:
        # Import the module
        spec = __import__(os.path.basename(dag_file_path).replace(".py", ""))

        # Find DAG objects in the module
        dags = []
        for attr_name in dir(spec):
            attr = getattr(spec, attr_name)
            if isinstance(attr, DAG):
                dags.append(attr)

        assert len(dags) > 0, f"No DAG objects found in {dag_file_path}"
        assert (
            len(dags) == 1
        ), f"Expected exactly one DAG, found {len(dags)} in {dag_file_path}"

        return dags[0]

    except Exception as e:
        pytest.fail(f"Failed to import DAG from {dag_file_path}: {str(e)}")


def assert_no_import_errors():
    """Test that all DAG files can be imported without errors"""
    dag_folder = os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
    dag_files = [
        f
        for f in os.listdir(dag_folder)
        if f.endswith(".py") and not f.startswith("__")
    ]

    for dag_file in dag_files:
        dag_path = os.path.join(dag_folder, dag_file)
        validate_dag_import(dag_path)
