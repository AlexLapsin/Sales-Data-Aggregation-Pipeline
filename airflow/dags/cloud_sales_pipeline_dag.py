# airflow/dags/cloud_sales_pipeline_dag.py
"""
Cloud Sales Data Pipeline DAG
Orchestrates the complete modern data pipeline:
1. Kafka streaming ingestion (monitoring)
2. Spark batch ETL processing
3. dbt transformations in Snowflake
4. Data quality monitoring and alerts
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

# ---------------- Configuration ----------------
default_args = {
    "owner": os.getenv("OWNER_NAME", "data-engineering"),
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "data-team@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),  # Pipeline should complete within 3 hours
}

# Environment variables for tasks
ENV_VARS = {
    # AWS Configuration
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    # S3 Configuration
    "S3_BUCKET": os.getenv("S3_BUCKET"),
    "PROCESSED_BUCKET": os.getenv("PROCESSED_BUCKET"),
    # Snowflake Configuration
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    # Databricks Configuration
    "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST"),
    "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN"),
    "DATABRICKS_CLUSTER_ID": os.getenv("DATABRICKS_CLUSTER_ID"),
    # Kafka Configuration
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "sales_events"),
}

# Docker image for pipeline tasks
PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "sales-pipeline:latest")


# ---------------- Helper Functions ----------------
def check_kafka_health(**context):
    """Check if Kafka streaming is healthy and producing data"""
    import subprocess
    import json

    try:
        # Check Kafka Connect status
        result = subprocess.run(
            ["curl", "-s", "http://localhost:8083/connectors/snowflake-sink/status"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            status = json.loads(result.stdout)
            connector_state = status.get("connector", {}).get("state")

            if connector_state == "RUNNING":
                context["task_instance"].xcom_push(key="kafka_status", value="healthy")
                return "spark_batch_processing"
            else:
                context["task_instance"].xcom_push(
                    key="kafka_status", value="unhealthy"
                )
                return "kafka_restart"
        else:
            context["task_instance"].xcom_push(key="kafka_status", value="error")
            return "kafka_restart"

    except Exception as e:
        print(f"Kafka health check failed: {e}")
        context["task_instance"].xcom_push(key="kafka_status", value="error")
        return "kafka_restart"


def check_data_freshness(**context):
    """Check if we have fresh data to process"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default",
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema=ENV_VARS["SNOWFLAKE_SCHEMA"],
    )

    # Check for recent streaming data
    streaming_query = """
    SELECT COUNT(*) as record_count
    FROM SALES_RAW
    WHERE PARTITION_DATE >= CURRENT_DATE() - INTERVAL '1 day'
    """

    # Check for recent batch data
    batch_query = """
    SELECT COUNT(*) as record_count
    FROM SALES_BATCH_RAW
    WHERE PARTITION_DATE >= CURRENT_DATE() - INTERVAL '1 day'
    """

    try:
        streaming_count = hook.get_first(streaming_query)[0] or 0
        batch_count = hook.get_first(batch_query)[0] or 0

        total_records = streaming_count + batch_count

        context["task_instance"].xcom_push(
            key="streaming_records", value=streaming_count
        )
        context["task_instance"].xcom_push(key="batch_records", value=batch_count)
        context["task_instance"].xcom_push(key="total_records", value=total_records)

        # Proceed if we have any data
        if total_records > 0:
            return "dbt_run_staging"
        else:
            print(
                f"No fresh data found. Streaming: {streaming_count}, Batch: {batch_count}"
            )
            return "skip_pipeline"

    except Exception as e:
        print(f"Data freshness check failed: {e}")
        # Proceed anyway in case of error
        return "dbt_run_staging"


# ---------------- DAG Definition ----------------
with DAG(
    dag_id="cloud_sales_pipeline",
    description="Modern cloud sales data pipeline with Kafka, Spark, dbt, and Snowflake",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cloud", "sales", "kafka", "spark", "dbt", "snowflake"],
    max_active_runs=1,  # Only one instance at a time
    params={
        "run_spark_job": True,
        "run_full_refresh": False,
        "skip_tests": False,
    },
) as dag:

    # ---------------- Pipeline Start ----------------
    start_pipeline = DummyOperator(
        task_id="start_pipeline",
        doc_md="""
        # Cloud Sales Pipeline Start

        This marks the beginning of the cloud-based sales data pipeline.
        The pipeline processes both streaming (Kafka) and batch (Spark) data sources.
        """,
    )

    # ---------------- Health Checks ----------------
    kafka_health_check = BranchPythonOperator(
        task_id="kafka_health_check",
        python_callable=check_kafka_health,
        doc_md="""
        Checks the health of Kafka Connect and streaming pipeline.
        Branches to restart if unhealthy, or continues if healthy.
        """,
    )

    kafka_restart = DockerOperator(
        task_id="kafka_restart",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/deploy/streaming && ./manage_connector.sh restart"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,  # Disable temporary directory mounting
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Restarts Kafka Connect if health check fails",
    )

    # ---------------- Spark Batch Processing ----------------
    spark_batch_processing = DatabricksRunNowOperator(
        task_id="spark_batch_processing",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_sales_etl_job_id }}",  # Set this variable in Airflow
        notebook_params={
            "input_path": f"s3://{ENV_VARS['S3_BUCKET']}/batch-data/{{{{ ds }}}}",
            "output_table": "SALES_BATCH_RAW",
            "batch_date": "{{ ds }}",
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Runs Spark ETL job on Databricks to process batch CSV files",
    )

    # Alternative: Run Spark job via Docker if Databricks is not available
    spark_batch_processing_docker = DockerOperator(
        task_id="spark_batch_processing_docker",
        image=PIPELINE_IMAGE,
        command=[
            "spark-submit",
            "--packages",
            "net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.11.3",
            "/app/src/spark/jobs/batch_etl.py",
            "--input-path",
            f"s3://{ENV_VARS['S3_BUCKET']}/batch-data/{{{{ ds }}}}",
            "--output-table",
            "SALES_BATCH_RAW",
        ],
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Alternative Spark processing via Docker container",
    )

    # ---------------- Data Quality Checks ----------------
    data_freshness_check = PythonOperator(
        task_id="data_freshness_check",
        python_callable=check_data_freshness,
        doc_md="Checks for fresh data in both streaming and batch sources",
    )

    skip_pipeline = DummyOperator(
        task_id="skip_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Skips pipeline execution when no fresh data is available",
    )

    # ---------------- dbt Transformations ----------------
    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt deps --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source=os.path.abspath("./dbt"), target="/app/dbt", type="bind")],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Installs dbt package dependencies",
    )

    dbt_run_staging = DockerOperator(
        task_id="dbt_run_staging",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt run --select staging --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source=os.path.abspath("./dbt"), target="/app/dbt", type="bind")],
        doc_md="Runs dbt staging models to clean and standardize raw data",
    )

    dbt_run_intermediate = DockerOperator(
        task_id="dbt_run_intermediate",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt run --select intermediate --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source=os.path.abspath("./dbt"), target="/app/dbt", type="bind")],
        doc_md="Runs dbt intermediate models for business logic and SCD2",
    )

    dbt_run_marts = DockerOperator(
        task_id="dbt_run_marts",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt run --select marts --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source=os.path.abspath("./dbt"), target="/app/dbt", type="bind")],
        doc_md="Runs dbt mart models to build final dimensional model",
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt test --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source=os.path.abspath("./dbt"), target="/app/dbt", type="bind")],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Runs comprehensive dbt tests for data quality validation",
    )

    # ---------------- Data Quality Monitoring ----------------
    quality_check_sql = """
    SELECT
        'fact_sales' as table_name,
        COUNT(*) as record_count,
        AVG(data_quality_score) as avg_quality_score,
        MIN(data_quality_score) as min_quality_score,
        COUNT(CASE WHEN data_quality_score < 80 THEN 1 END) as low_quality_records
    FROM SALES_DW.MARTS.FACT_SALES
    WHERE DATE_KEY >= TO_NUMBER(TO_CHAR(CURRENT_DATE() - INTERVAL '1 day', 'YYYYMMDD'))

    UNION ALL

    SELECT
        'fact_sales_daily' as table_name,
        COUNT(*) as record_count,
        AVG(avg_data_quality_score) as avg_quality_score,
        MIN(avg_data_quality_score) as min_quality_score,
        COUNT(CASE WHEN avg_data_quality_score < 70 THEN 1 END) as low_quality_records
    FROM SALES_DW.MARTS.FACT_SALES_DAILY
    WHERE DATE_KEY >= TO_NUMBER(TO_CHAR(CURRENT_DATE() - INTERVAL '1 day', 'YYYYMMDD'))
    """

    data_quality_monitor = SnowflakeOperator(
        task_id="data_quality_monitor",
        snowflake_conn_id="snowflake_default",
        sql=quality_check_sql,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="MARTS",
        doc_md="Monitors data quality metrics and identifies issues",
    )

    # ---------------- Documentation Generation ----------------
    dbt_docs_generate = DockerOperator(
        task_id="dbt_docs_generate",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt docs generate --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source=os.path.abspath("./dbt"), target="/app/dbt", type="bind")],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Generates fresh dbt documentation",
    )

    # ---------------- Success Notification ----------------
    success_notification = SlackWebhookOperator(
        task_id="success_notification",
        http_conn_id="slack_webhook",
        message="""
        Cloud Sales Pipeline Completed Successfully!

        Processed Data:
        - Streaming Records: {{ ti.xcom_pull(key='streaming_records', task_ids='data_freshness_check') }}
        - Batch Records: {{ ti.xcom_pull(key='batch_records', task_ids='data_freshness_check') }}
        - Total Records: {{ ti.xcom_pull(key='total_records', task_ids='data_freshness_check') }}

        🕒 Execution Date: {{ ds }}
        Duration: {{ (ti.end_date - ti.start_date).total_seconds() }} seconds

        📈 Pipeline Status: All stages completed successfully
        Data Quality: Monitored and validated
        📚 Documentation: Updated and available
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ---------------- Failure Handling ----------------
    failure_notification = SlackWebhookOperator(
        task_id="failure_notification",
        http_conn_id="slack_webhook",
        message="""
        ❌ Cloud Sales Pipeline Failed!

        🕒 Execution Date: {{ ds }}
        Failed Task: {{ ti.task_id }}
        📝 Error Details: Check Airflow logs for more information

        Action Required: Please investigate and resolve the issue
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ---------------- Pipeline End ----------------
    end_pipeline = DummyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Marks the successful completion of the cloud sales pipeline",
    )

    # ---------------- Task Dependencies ----------------

    # Pipeline start and health checks
    start_pipeline >> kafka_health_check
    kafka_health_check >> [kafka_restart, spark_batch_processing]
    kafka_restart >> spark_batch_processing

    # Alternative Spark processing paths
    spark_batch_processing >> data_freshness_check
    # Uncomment to use Docker Spark instead of Databricks:
    # kafka_health_check >> spark_batch_processing_docker >> data_freshness_check

    # Data freshness branching
    data_freshness_check >> [skip_pipeline, dbt_deps]
    skip_pipeline >> end_pipeline

    # dbt transformation chain
    dbt_deps >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts
    dbt_run_marts >> [dbt_test, data_quality_monitor]

    # Documentation and final steps
    [dbt_test, data_quality_monitor] >> dbt_docs_generate
    dbt_docs_generate >> success_notification >> end_pipeline

    # Failure handling
    [
        kafka_health_check,
        spark_batch_processing,
        dbt_run_staging,
        dbt_run_intermediate,
        dbt_run_marts,
        dbt_test,
    ] >> failure_notification
    failure_notification >> end_pipeline
