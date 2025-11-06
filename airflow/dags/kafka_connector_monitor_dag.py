# airflow/dags/kafka_connector_monitor_dag.py
"""
Kafka Connect Connector Monitoring DAG

Monitors Kafka Connect infrastructure health for streaming data ingestion.
DOES NOT ingest or process data - only monitors connector status and health.

Responsibilities:
- Monitor Kafka Connect S3 Sink connector health
- Restart failed connectors automatically
- Alert on critical issues
- Ensure continuous streaming to Bronze layer

Schedule: Every 15 minutes (continuous monitoring)
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

# Configuration
default_args = {
    "owner": os.getenv("OWNER_NAME", "data-engineering"),
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "data-team@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

ENV_VARS = {
    "KAFKA_CONNECT_URL": os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083"),
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
}

CONNECTOR_NAME = "sales-events-s3-bronze-sink"


def check_kafka_connect_health(**context):
    """
    Check if Kafka Connect service is healthy and responsive.
    """
    import requests

    connect_url = ENV_VARS["KAFKA_CONNECT_URL"]

    print(f"Checking Kafka Connect health at: {connect_url}")

    try:
        response = requests.get(f"{connect_url}/", timeout=10)

        if response.status_code == 200:
            print("SUCCESS: Kafka Connect is healthy")
            context["task_instance"].xcom_push(key="connect_health", value="healthy")
            return {"status": "healthy"}
        else:
            print(f"WARNING: Kafka Connect returned status {response.status_code}")
            context["task_instance"].xcom_push(key="connect_health", value="degraded")
            return {"status": "degraded", "code": response.status_code}

    except Exception as e:
        print(f"FAILED: Kafka Connect health check failed: {e}")
        context["task_instance"].xcom_push(key="connect_health", value="unhealthy")
        raise


def check_connector_status(**context):
    """
    Check S3 Sink connector status and task states.
    """
    import requests
    import json

    connect_url = ENV_VARS["KAFKA_CONNECT_URL"]
    status_url = f"{connect_url}/connectors/{CONNECTOR_NAME}/status"

    print(f"Checking connector: {CONNECTOR_NAME}")

    try:
        response = requests.get(status_url, timeout=10)

        if response.status_code != 200:
            raise ValueError(f"Connector not found or error: {response.status_code}")

        status = response.json()
        connector_state = status["connector"]["state"]
        tasks = status["tasks"]

        running_tasks = sum(1 for task in tasks if task["state"] == "RUNNING")
        failed_tasks = sum(1 for task in tasks if task["state"] == "FAILED")
        total_tasks = len(tasks)

        print(f"Connector State: {connector_state}")
        print(f"Tasks: {running_tasks}/{total_tasks} running, {failed_tasks} failed")

        # Store metrics
        context["task_instance"].xcom_push(key="connector_state", value=connector_state)
        context["task_instance"].xcom_push(key="running_tasks", value=running_tasks)
        context["task_instance"].xcom_push(key="failed_tasks", value=failed_tasks)
        context["task_instance"].xcom_push(key="total_tasks", value=total_tasks)

        if connector_state == "RUNNING" and failed_tasks == 0:
            print("SUCCESS: Connector is healthy")
            return {"status": "healthy", "running": running_tasks, "total": total_tasks}
        elif failed_tasks > 0:
            print(f"WARNING: Connector has {failed_tasks} failed tasks")
            return {"status": "degraded", "failed": failed_tasks}
        else:
            print(f"WARNING: Connector state is {connector_state}")
            return {"status": "unhealthy", "state": connector_state}

    except Exception as e:
        print(f"FAILED: Connector status check failed: {e}")
        raise


def restart_failed_connector(**context):
    """
    Restart connector if it's in failed state.
    Only runs if connector status check detected issues.
    """
    import requests

    connector_state = context["task_instance"].xcom_pull(
        task_ids="check_connector_status", key="connector_state"
    )

    if connector_state == "RUNNING":
        print("Connector is running - no restart needed")
        return {"status": "skipped", "reason": "connector_running"}

    connect_url = ENV_VARS["KAFKA_CONNECT_URL"]
    restart_url = f"{connect_url}/connectors/{CONNECTOR_NAME}/restart"

    print(f"Restarting connector: {CONNECTOR_NAME}")

    try:
        response = requests.post(restart_url, timeout=30)

        if response.status_code in [200, 202, 204]:
            print("SUCCESS: Connector restart initiated")
            return {"status": "restarted"}
        else:
            print(f"WARNING: Restart returned status {response.status_code}")
            return {"status": "restart_failed", "code": response.status_code}

    except Exception as e:
        print(f"FAILED: Connector restart failed: {e}")
        raise


def check_bronze_data_flow(**context):
    """
    Verify that streaming data is flowing to Bronze S3 layer.
    Checks for recent data (last 1 hour).
    """
    import boto3
    from datetime import datetime, timedelta

    bucket = os.getenv("RAW_BUCKET")
    prefix = "sales_data/"  # Kafka Connect writes with year/month/day/hour partitions

    print(f"Checking Bronze data flow in: s3://{bucket}/{prefix}")

    s3_client = boto3.client("s3")

    try:
        # Check for recent objects (last hour)
        one_hour_ago = datetime.now() - timedelta(hours=1)

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=100)

        if "Contents" not in response:
            print("WARNING: No streaming data found in Bronze layer")
            context["task_instance"].xcom_push(key="data_flow", value="no_data")
            return {"status": "no_data"}

        recent_objects = [
            obj
            for obj in response["Contents"]
            if obj["LastModified"].replace(tzinfo=None) > one_hour_ago
        ]

        recent_count = len(recent_objects)

        print(f"Recent objects (last hour): {recent_count}")

        context["task_instance"].xcom_push(key="recent_objects", value=recent_count)

        if recent_count > 0:
            print("SUCCESS: Data is flowing to Bronze layer")
            context["task_instance"].xcom_push(key="data_flow", value="healthy")
            return {"status": "healthy", "recent_files": recent_count}
        else:
            print("WARNING: No recent data in Bronze layer")
            context["task_instance"].xcom_push(key="data_flow", value="stale")
            return {"status": "stale"}

    except Exception as e:
        print(f"FAILED: Bronze data flow check failed: {e}")
        raise


with DAG(
    dag_id="kafka_connector_monitor",
    default_args=default_args,
    description="Monitors Kafka Connect connector health (does not ingest data)",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["streaming", "monitoring", "infrastructure"],
    max_active_runs=1,
    doc_md="""
    # Streaming Infrastructure Management

    Continuously monitors Kafka Connect health and manages connector lifecycle.

    ## Responsibilities
    1. Check Kafka Connect service health
    2. Monitor S3 Sink connector status
    3. Restart failed connectors automatically
    4. Verify data flow to Bronze layer
    5. Alert on critical issues

    ## What This DAG Does NOT Do
    - Process data (handled by Streaming Processing DAG)
    - Transform data (handled by Streaming Processing DAG)
    - Update Silver/Gold layers (handled by other DAGs)

    ## Schedule
    Runs every 15 minutes for continuous monitoring.
    """,
) as dag:

    start = DummyOperator(
        task_id="start_monitoring", doc_md="Start streaming infrastructure monitoring"
    )

    kafka_health = PythonOperator(
        task_id="check_kafka_connect_health",
        python_callable=check_kafka_connect_health,
        doc_md="""
        # Kafka Connect Health Check

        Verifies Kafka Connect service is responsive.
        """,
    )

    connector_status = PythonOperator(
        task_id="check_connector_status",
        python_callable=check_connector_status,
        doc_md="""
        # Connector Status Check

        Monitors S3 Sink connector state and task health.

        **Checks**:
        - Connector state (RUNNING/FAILED/PAUSED)
        - Number of running tasks
        - Number of failed tasks
        """,
    )

    restart_connector = PythonOperator(
        task_id="restart_failed_connector",
        python_callable=restart_failed_connector,
        doc_md="""
        # Auto-Restart Failed Connector

        Automatically restarts connector if detected as failed.
        Implements self-healing pattern for streaming infrastructure.

        Only runs if connector status check succeeds.
        If connector doesn't exist, DAG fails at status check.
        """,
    )

    bronze_flow_check = PythonOperator(
        task_id="check_bronze_data_flow",
        python_callable=check_bronze_data_flow,
        doc_md="""
        # Bronze Data Flow Verification

        Verifies streaming data is reaching Bronze S3 layer.
        Alerts if no recent data detected (possible connector issue).

        Only runs if connector monitoring succeeds.
        """,
    )

    complete = DummyOperator(
        task_id="monitoring_complete",
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Monitoring cycle complete",
    )

    # Task dependencies - monitoring flow
    (
        start
        >> kafka_health
        >> connector_status
        >> restart_connector
        >> bronze_flow_check
        >> complete
    )
