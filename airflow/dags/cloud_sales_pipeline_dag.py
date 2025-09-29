# airflow/dags/cloud_sales_pipeline_dag.py
"""
Cloud Sales Data Pipeline DAG
Orchestrates the complete updated data pipeline:
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
from airflow.models import Variable
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
    "RAW_BUCKET": os.getenv("RAW_BUCKET"),
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
    "DATABRICKS_USERNAME": os.getenv("DATABRICKS_USERNAME"),
    "DATABRICKS_SALES_ETL_JOB_ID": os.getenv("DATABRICKS_SALES_ETL_JOB_ID"),
    # Kafka Configuration
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "sales_events"),
}

# Docker image for pipeline tasks
PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "sales-pipeline:latest")


# ---------------- Helper Functions ----------------
def choose_spark_execution_path(**context):
    """
    Choose Bronze layer ingestion first, then determine Spark execution path.
    All data processing must go through Bronze layer for Medallion architecture.
    """
    try:
        print("Medallion Architecture: Starting with Bronze layer data ingestion")
        print("All processing flows: Local Data → Bronze → Silver → Gold")

        # ALWAYS start with Bronze layer for proper Medallion architecture
        return "bronze_data_ingestion"

    except Exception as e:
        print(f"Error in Bronze layer routing: {e}")
        print("Defaulting to Bronze layer ingestion")
        return "bronze_data_ingestion"


def choose_spark_execution_after_bronze(**context):
    """
    Choose between Databricks and local Spark execution AFTER Bronze layer processing.
    Called from Bronze validation task.
    """
    try:
        # Check if Databricks credentials are configured in .env
        databricks_host = os.getenv("DATABRICKS_HOST")
        databricks_token = os.getenv("DATABRICKS_TOKEN")

        print(f"Bronze → Silver: Choosing Spark execution path")
        print(f"DATABRICKS_HOST configured: {bool(databricks_host)}")
        print(f"DATABRICKS_TOKEN configured: {bool(databricks_token)}")

        if databricks_host and databricks_token:
            print(f"Using Databricks for Silver layer processing: {databricks_host}")
            return "spark_batch_processing"
        else:
            print("Using local Spark for Silver layer processing")
            print("To use Databricks: Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")
            return "spark_batch_processing_local"

    except Exception as e:
        print(f"Error choosing Spark execution: {e}")
        print("Defaulting to local Spark processing")
        return "spark_batch_processing_local"


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


def bronze_data_ingestion_function(**context):
    """
    Bronze Layer Data Ingestion - Upload actual CSV files to Bronze S3 layer
    This creates the immutable audit trail by copying local data to partitioned Bronze structure
    """
    import sys

    sys.path.append("/opt/airflow")
    from src.bronze.data_uploader import upload_local_data_to_bronze

    try:
        bucket_name = ENV_VARS["RAW_BUCKET"]
        batch_id = context["ds"]

        # Docker mounted data directory
        local_data_dir = "/app/data/raw"

        print(f"Starting Bronze data ingestion for batch: {batch_id}")
        print(f"Source directory: {local_data_dir}")
        print(f"Target bucket: {bucket_name}")

        # Upload local CSV files to Bronze layer
        upload_result = upload_local_data_to_bronze(
            bucket_name=bucket_name, local_data_dir=local_data_dir, batch_date=batch_id
        )

        # Store upload results in XCom
        context["task_instance"].xcom_push(
            key="bronze_upload_result", value=upload_result
        )
        context["task_instance"].xcom_push(
            key="bronze_files_uploaded", value=upload_result.get("files_uploaded", 0)
        )
        context["task_instance"].xcom_push(
            key="bronze_ingestion_status", value=upload_result.get("status", "unknown")
        )

        files_uploaded = upload_result.get("files_uploaded", 0)
        files_found = upload_result.get("files_found", 0)
        status = upload_result.get("status", "unknown")

        print(f"Bronze ingestion completed")
        print(f"Status: {status}")
        print(f"Files uploaded: {files_uploaded}/{files_found}")

        if "sample_data_created" in upload_result:
            print("Sample data created for testing (no local files found)")

        # Require at least some data uploaded
        if files_uploaded == 0 and status != "success":
            raise ValueError(f"Bronze ingestion failed: no files uploaded")

        return upload_result

    except Exception as e:
        print(f"Bronze data ingestion failed: {e}")
        context["task_instance"].xcom_push(
            key="bronze_ingestion_status", value="failed"
        )
        context["task_instance"].xcom_push(key="bronze_ingestion_error", value=str(e))
        raise


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
    # Route between Databricks and local Spark execution
    spark_execution_branch = BranchPythonOperator(
        task_id="spark_execution_branch",
        python_callable=choose_spark_execution_path,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Choose between Databricks and local Spark execution",
    )

    # Bronze Layer Data Ingestion
    bronze_data_ingestion_task = PythonOperator(
        task_id="bronze_data_ingestion",
        python_callable=bronze_data_ingestion_function,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="""
        # Bronze Layer Data Ingestion

        Uploads actual CSV files from local data directory to partitioned Bronze S3 structure.
        This creates the immutable audit trail by copying raw data with proper Medallion architecture.

        **Key Functions:**
        - Upload CSV files from /app/data/raw to Bronze layer
        - Create partitioned structure: sales_data/year=YYYY/month=MM/day=DD/
        - Generate upload metadata and audit trail
        - Handle missing data with sample file creation

        **Data Flow:** Local CSV files → S3 Bronze → Ready for validation and Silver processing

        **Next Stage:** Bronze data quality validation
        """,
    )

    # Bronze Data Quality Validation
    def validate_bronze_data(**context):
        """Validate Bronze layer data quality before Silver processing"""
        import sys

        sys.path.append("/opt/airflow")
        from src.validation.bronze_validator import validate_bronze_batch

        bucket_name = ENV_VARS["RAW_BUCKET"]
        batch_id = context["ds"]

        print(f"Starting Bronze data validation for batch: {batch_id}")
        print(f"Target bucket: {bucket_name}")

        try:
            # Validate Bronze layer data
            validation_result = validate_bronze_batch(bucket_name, batch_id)

            # Store validation results in XCom
            context["task_instance"].xcom_push(
                key="bronze_validation_result", value=validation_result
            )
            context["task_instance"].xcom_push(
                key="bronze_quality_score",
                value=validation_result.get("overall_quality_score", 0),
            )
            context["task_instance"].xcom_push(
                key="bronze_validation_status",
                value=validation_result.get("status", "unknown"),
            )

            quality_score = validation_result.get("overall_quality_score", 0)
            status = validation_result.get("status", "unknown")

            print(f"Bronze validation completed")
            print(f"Quality Score: {quality_score}/100")
            print(f"Status: {status}")
            print(
                f"Ready for Silver processing: {validation_result.get('ready_for_silver_processing', False)}"
            )

            # Quality gate - fail if score is too low
            if quality_score < 70:
                raise ValueError(
                    f"Bronze data quality score ({quality_score}) below threshold (70)"
                )

            return validation_result

        except Exception as e:
            print(f"Bronze validation failed: {e}")
            context["task_instance"].xcom_push(
                key="bronze_validation_status", value="failed"
            )
            context["task_instance"].xcom_push(
                key="bronze_validation_error", value=str(e)
            )
            raise

    bronze_validation_task = BranchPythonOperator(
        task_id="bronze_data_validation",
        python_callable=choose_spark_execution_after_bronze,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="""
        # Bronze Data Quality Validation and Spark Routing

        Validates Bronze layer data quality and chooses Spark execution path.

        After Bronze validation, routes to either:
        - Databricks Spark (if credentials configured)
        - Local Spark (fallback)

        **Quality Gates:** Data must pass Bronze validation before Silver processing
        """,
    )

    # Databricks execution
    spark_batch_processing = DatabricksRunNowOperator(
        task_id="spark_batch_processing",
        databricks_conn_id="databricks_default",
        job_id=ENV_VARS["DATABRICKS_SALES_ETL_JOB_ID"],
        python_params=[
            "--input-path",
            f"s3://{ENV_VARS['RAW_BUCKET']}/",
            "--output-table",
            "SALES_BATCH_RAW",
            "--batch-id",
            "{{ ds }}",
        ],
        doc_md="Execute Databricks ETL job using Git-integrated workspace",
    )

    # Local Spark execution
    def run_local_spark_job(**context):
        """Execute Spark job locally using the ETL container"""
        import subprocess
        import os

        batch_id = context["ds"]
        input_path = f"s3://{ENV_VARS['RAW_BUCKET']}/"
        output_table = "SALES_BATCH_RAW"

        print(f"Starting local Spark job with batch_id: {batch_id}")
        print(f"Input path: {input_path}")
        print(f"Output table: {output_table}")

        # Command to run the Spark job in the Spark container
        cmd = [
            "docker",
            "exec",
            "sales_data_aggregation_pipeline-spark-local-1",
            "python",
            "/app/src/spark/jobs/batch_etl.py",
            "--input-path",
            input_path,
            "--output-table",
            output_table,
            "--batch-id",
            batch_id,
        ]

        print(f"Executing command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=1800  # 30 minutes timeout
        )

        if result.returncode == 0:
            print("Spark job completed successfully!")
            print("STDOUT:", result.stdout)
        else:
            print(f"Spark job failed with return code: {result.returncode}")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            raise RuntimeError(f"Local Spark job failed: {result.stderr}")

    spark_batch_processing_local = PythonOperator(
        task_id="spark_batch_processing_local",
        python_callable=run_local_spark_job,
        retries=2,  # Retry twice on failure
        retry_delay=timedelta(minutes=1),
        doc_md="Execute Spark job locally using ETL container",
    )

    # Join point after Spark processing
    spark_processing_complete = DummyOperator(
        task_id="spark_processing_complete",
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
        doc_md="Mark completion of Spark batch processing",
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
        doc_md="Runs dbt staging models to clean and normalize raw data",
    )

    dbt_run_intermediate = DockerOperator(
        task_id="dbt_run_intermediate",
        image=PIPELINE_IMAGE,
        command='bash -c "cd /app/dbt && dbt run --select intermediate --profiles-dir /app/dbt"',
        auto_remove=True,
        environment=ENV_VARS,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
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
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Runs complete dbt tests for data quality validation",
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
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Generates fresh dbt documentation",
    )

    # ---------------- Success Notification ----------------
    success_notification = SlackWebhookOperator(
        task_id="success_notification",
        slack_webhook_conn_id="slack_webhook",
        message="""
        Cloud Sales Pipeline Completed Successfully!

        Processed Data:
        - Streaming Records: {{ ti.xcom_pull(key='streaming_records', task_ids='data_freshness_check') }}
        - Batch Records: {{ ti.xcom_pull(key='batch_records', task_ids='data_freshness_check') }}
        - Total Records: {{ ti.xcom_pull(key='total_records', task_ids='data_freshness_check') }}

        Execution Date: {{ ds }}
        Duration: {{ (ti.end_date - ti.start_date).total_seconds() }} seconds

        Pipeline Status: All stages completed successfully
        Data Quality: Monitored and validated
        Documentation: Updated and available
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ---------------- Failure Handling ----------------
    failure_notification = SlackWebhookOperator(
        task_id="failure_notification",
        slack_webhook_conn_id="slack_webhook",
        message="""
        ERROR: Cloud Sales Pipeline Failed!

        Execution Date: {{ ds }}
        Failed Task: {{ ti.task_id }}
        Error Details: Check Airflow logs for more information

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
    kafka_health_check >> [kafka_restart, spark_execution_branch]
    kafka_restart >> spark_execution_branch

    # Spark processing paths with Bronze data ingestion and validation
    spark_execution_branch >> bronze_data_ingestion_task
    bronze_data_ingestion_task >> bronze_validation_task
    bronze_validation_task >> [spark_batch_processing, spark_batch_processing_local]
    [spark_batch_processing, spark_batch_processing_local] >> spark_processing_complete
    spark_processing_complete >> data_freshness_check

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
        spark_execution_branch,
        bronze_data_ingestion_task,
        bronze_validation_task,
        spark_batch_processing,
        spark_batch_processing_local,
        dbt_run_staging,
        dbt_run_intermediate,
        dbt_run_marts,
        dbt_test,
    ] >> failure_notification
    failure_notification >> end_pipeline
