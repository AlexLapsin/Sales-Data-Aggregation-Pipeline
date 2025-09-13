"""
Pytest configuration and fixtures for Airflow DAG testing
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any
from unittest.mock import Mock, patch, MagicMock
import pytest
from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable, Connection
from airflow.utils.dates import days_ago

# Add the project root to Python path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "airflow", "dags"))

from tests.airflow.test_utils import (
    MockAirflowContext,
    MockedEnvironmentVariables,
    MockSnowflakeHook,
    get_test_env_vars,
)


@pytest.fixture(scope="session", autouse=True)
def setup_airflow_test_environment():
    """Setup Airflow test environment"""
    # Set Airflow configuration for testing
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"

    # Initialize Airflow database in memory for testing
    from airflow.utils.db import initdb

    try:
        initdb()
    except:
        # Database might already be initialized
        pass


@pytest.fixture
def test_env_vars():
    """Fixture for test environment variables"""
    env_vars = get_test_env_vars()
    with MockedEnvironmentVariables(env_vars):
        yield env_vars


@pytest.fixture
def mock_airflow_context():
    """Fixture for mock Airflow context"""
    return MockAirflowContext()


@pytest.fixture
def mock_airflow_context_with_xcom():
    """Fixture for mock Airflow context with XCom data"""
    context = MockAirflowContext()

    # Setup XCom mock data
    xcom_data = {
        "kafka_status": "healthy",
        "streaming_records": 1000,
        "batch_records": 500,
        "total_records": 1500,
        "daily_report": {
            "total_transactions": 1000,
            "total_sales": 50000.0,
            "unique_customers": 250,
            "avg_quality_score": 85.5,
            "transaction_growth": 5.2,
            "sales_growth": 8.1,
        },
        "quality_issues": [],
        "table_recommendations": [
            {
                "table": "MARTS.FACT_SALES",
                "action": "add_clustering",
                "reason": "Large table (5.2 GB) without clustering",
            }
        ],
    }

    def mock_xcom_pull(key=None, task_ids=None):
        if key:
            return xcom_data.get(key)
        return xcom_data

    context.task_instance.xcom_pull.side_effect = mock_xcom_pull
    return context


@pytest.fixture
def mock_snowflake_hook():
    """Fixture for mock Snowflake hook"""
    # Sample data for different test scenarios
    quality_check_results = [
        ("fact_sales", 10000, 85.5, 80.0, 50),
        ("fact_sales_daily", 365, 82.3, 75.0, 12),
    ]

    daily_metrics_result = [
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

    quality_trend_records = [
        (datetime(2024, 1, 1), 85.5, 1000, 50, 5.0),
        (datetime(2024, 1, 2), 83.2, 1100, 80, 7.3),
        (datetime(2024, 1, 3), 87.1, 950, 30, 3.2),
    ]

    table_stats_records = [
        ("MARTS", "FACT_SALES", 1000000, 5368709120, 5.0, None, False),
        ("MARTS", "DIM_PRODUCT", 10000, 104857600, 0.1, "PRODUCT_ID", True),
        ("RAW", "SALES_RAW", 5000000, 21474836480, 20.0, None, False),
    ]

    return MockSnowflakeHook(
        results=[daily_metrics_result],
        records=quality_trend_records + table_stats_records,
    )


@pytest.fixture
def mock_databricks_connection():
    """Fixture for mock Databricks connection"""
    with patch(
        "airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator"
    ) as mock_op:
        mock_instance = Mock()
        mock_instance.execute.return_value = {"run_id": 12345}
        mock_op.return_value = mock_instance
        yield mock_op


@pytest.fixture
def mock_docker_operator():
    """Fixture for mock Docker operator"""
    with patch("airflow.providers.docker.operators.docker.DockerOperator") as mock_op:
        mock_instance = Mock()
        mock_instance.execute.return_value = "Docker command executed successfully"
        mock_op.return_value = mock_instance
        yield mock_op


@pytest.fixture
def mock_snowflake_operator():
    """Fixture for mock Snowflake operator"""
    with patch(
        "airflow.providers.snowflake.operators.snowflake.SnowflakeOperator"
    ) as mock_op:
        mock_instance = Mock()
        mock_instance.execute.return_value = "Snowflake query executed successfully"
        mock_op.return_value = mock_instance
        yield mock_op


@pytest.fixture
def mock_slack_operator():
    """Fixture for mock Slack operator"""
    with patch(
        "airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator"
    ) as mock_op:
        mock_instance = Mock()
        mock_instance.execute.return_value = "Slack message sent"
        mock_op.return_value = mock_instance
        yield mock_op


@pytest.fixture
def mock_email_operator():
    """Fixture for mock Email operator"""
    with patch("airflow.operators.email.EmailOperator") as mock_op:
        mock_instance = Mock()
        mock_instance.execute.return_value = "Email sent successfully"
        mock_op.return_value = mock_instance
        yield mock_op


@pytest.fixture
def mock_bash_operator():
    """Fixture for mock Bash operator"""
    with patch("airflow.operators.bash.BashOperator") as mock_op:
        mock_instance = Mock()
        mock_instance.execute.return_value = "Bash command executed"
        mock_op.return_value = mock_instance
        yield mock_op


@pytest.fixture
def mock_external_services():
    """Fixture for mocking all external services"""
    patches = []

    # Mock subprocess for Kafka health checks
    subprocess_patch = patch("subprocess.run")
    mock_subprocess = subprocess_patch.start()
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = '{"connector":{"state":"RUNNING"}}'
    mock_subprocess.return_value = mock_result
    patches.append(subprocess_patch)

    # Mock Snowflake hook
    snowflake_patch = patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    mock_hook = snowflake_patch.start()
    mock_hook.return_value = MockSnowflakeHook()
    patches.append(snowflake_patch)

    yield

    # Clean up patches
    for patch_obj in patches:
        patch_obj.stop()


@pytest.fixture
def dag_execution_date():
    """Standard execution date for DAG testing"""
    return datetime(2024, 1, 1, 2, 0, 0)  # 2 AM on Jan 1, 2024


@pytest.fixture
def mock_airflow_variables():
    """Mock Airflow Variables"""
    with patch.object(Variable, "get") as mock_var:
        mock_var.side_effect = lambda key, default_var=None: {
            "databricks_sales_etl_job_id": "test_job_id_12345",
            "slack_webhook_url": "https://hooks.slack.com/test",
            "email_recipients": "test@company.com",
        }.get(key, default_var)
        yield mock_var


@pytest.fixture
def mock_airflow_connections():
    """Mock Airflow Connections"""
    with patch("airflow.models.Connection.get_connection_from_secrets") as mock_conn:

        def get_connection(conn_id):
            connections = {
                "snowflake_default": Mock(
                    host="test-account.snowflakecomputing.com",
                    login="test_user",
                    password="test_password",
                ),
                "databricks_default": Mock(
                    host="https://test.databricks.com", password="test_token"
                ),
                "slack_webhook": Mock(
                    host="hooks.slack.com", password="test_webhook_url"
                ),
            }
            return connections.get(conn_id, Mock())

        mock_conn.side_effect = get_connection
        yield mock_conn


@pytest.fixture(autouse=True)
def setup_dag_test_environment(
    test_env_vars,
    mock_external_services,
    mock_airflow_variables,
    mock_airflow_connections,
):
    """Auto-use fixture that sets up the complete test environment"""
    # This fixture automatically runs for all tests and sets up the basic environment
    pass


@pytest.fixture
def sample_dag():
    """Sample DAG for testing common functionality"""
    dag = DAG(
        dag_id="test_dag",
        description="Test DAG for unit tests",
        schedule_interval="@daily",
        start_date=days_ago(1),
        catchup=False,
        tags=["test"],
    )
    return dag


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line("markers", "unit: mark test as unit test")
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line(
        "markers", "external: mark test as requiring external services"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names/paths"""
    for item in items:
        # Add unit marker by default
        if "unit" not in [mark.name for mark in item.iter_markers()]:
            item.add_marker(pytest.mark.unit)

        # Add integration marker for integration tests
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)

        # Add slow marker for performance tests
        if "performance" in item.nodeid:
            item.add_marker(pytest.mark.slow)

        # Add external marker for tests requiring external services
        if any(
            keyword in item.nodeid for keyword in ["databricks", "snowflake", "kafka"]
        ):
            item.add_marker(pytest.mark.external)
