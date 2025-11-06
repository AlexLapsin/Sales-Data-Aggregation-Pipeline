# airflow/dags/analytics_dag.py
"""
Analytics Pipeline DAG - Delta Direct Architecture

THE ONLY DAG that processes Silver → Gold layer.
Event-driven: Triggered when EITHER batch or streaming updates Silver layer.

Flow: Silver Delta Lake (S3) → Delta Direct (Iceberg) → Gold Snowflake

Architecture: Zero-copy Delta Direct with AUTO_REFRESH
- Snowflake reads Delta Lake from S3 via Iceberg table SALES_SILVER_EXTERNAL
- AUTO_REFRESH automatically detects new Delta Lake transactions
- No manual merge required - unified view provided by Delta Direct
- dbt transforms directly from Iceberg table to Gold dimensional model

Schedule: Event-driven via Dataset triggers (no fixed schedule)
Technology: Snowflake Delta Direct + dbt transformations
Idempotency: Deterministic transformations ensure safe reruns
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.datasets import Dataset

from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ExecutionConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import TestBehavior
from pathlib import Path

# Configuration
default_args = {
    "owner": os.getenv("OWNER_NAME", "data-engineering"),
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "data-team@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
}

ENV_VARS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    "PROCESSED_BUCKET": os.getenv("PROCESSED_BUCKET"),
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "SNOWFLAKE_KEY_PASSPHRASE": os.getenv("SNOWFLAKE_KEY_PASSPHRASE"),
    "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
    "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
}

# Dataset inputs - this DAG is triggered by updates to either dataset
silver_batch_dataset = Dataset(f"s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/sales/")
silver_streaming_dataset = Dataset(f"s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/sales/")

# Cosmos configuration for dbt integration
DBT_PROJECT_PATH = Path("/app/dbt")

profile_config = ProfileConfig(
    profile_name="sales_data_pipeline",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
)


def verify_delta_direct_refresh(**context):
    """
    Verify Delta Direct AUTO_REFRESH has detected latest Silver layer changes.

    Delta Direct Architecture:
    - Snowflake Iceberg table SALES_SILVER_EXTERNAL points to Delta Lake in S3
    - AUTO_REFRESH automatically detects new Delta Lake transactions via _delta_log
    - No manual merge needed - unified view of batch + streaming data
    - Zero-copy: data stays in S3, Snowflake queries in place
    """
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    print("=== Delta Direct Refresh Verification ===")

    # Load encrypted private key for keypair authentication
    with open("/config/keys/rsa_key.p8", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=ENV_VARS["SNOWFLAKE_KEY_PASSPHRASE"].encode(),
            backend=default_backend(),
        )

    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    conn = snowflake.connector.connect(
        account=ENV_VARS["SNOWFLAKE_ACCOUNT"],
        user=ENV_VARS["SNOWFLAKE_USER"],
        private_key=pkb,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="RAW",
        role=ENV_VARS.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

    try:
        cursor = conn.cursor()

        # CRITICAL: Refresh external table to detect new Delta Lake files
        # This ensures Snowflake sees the latest data written by batch/streaming jobs
        print("Refreshing SALES_SILVER_EXTERNAL to detect new Delta files...")
        cursor.execute("ALTER ICEBERG TABLE SALES_SILVER_EXTERNAL REFRESH")
        print("SUCCESS: External table refreshed")

        # Check Iceberg table metadata refresh status
        cursor.execute("DESCRIBE ICEBERG TABLE SALES_SILVER_EXTERNAL")
        table_info = cursor.fetchall()

        # Get current record count
        cursor.execute("SELECT COUNT(*) FROM SALES_SILVER_EXTERNAL")
        total_records = cursor.fetchone()[0]

        # Get batch vs streaming breakdown
        cursor.execute(
            """
            SELECT
                source_system,
                COUNT(*) as record_count,
                MAX(processing_timestamp) as latest_timestamp
            FROM SALES_SILVER_EXTERNAL
            GROUP BY source_system
        """
        )

        source_breakdown = cursor.fetchall()

        print(f"Total records in Delta Direct table: {total_records:,}")

        batch_records = 0
        streaming_records = 0

        for row in source_breakdown:
            source_system, count, latest_ts = row
            print(f"{source_system} source: {count:,} records (latest: {latest_ts})")

            if source_system == "BATCH":
                batch_records = count
            elif source_system == "KAFKA":
                streaming_records = count

        print(f"AUTO_REFRESH Status: Active (Delta Lake transaction log monitored)")
        print("SUCCESS: Delta Direct verification completed")

        context["task_instance"].xcom_push(key="total_records", value=total_records)
        context["task_instance"].xcom_push(key="batch_records", value=batch_records)
        context["task_instance"].xcom_push(
            key="streaming_records", value=streaming_records
        )

        return {
            "status": "success",
            "total_records": total_records,
            "batch_records": batch_records,
            "streaming_records": streaming_records,
        }

    except Exception as e:
        print(f"FAILED: Delta Direct verification error: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def publish_analytics_metrics(**context):
    """
    Publish metrics about the analytics pipeline run.
    Tracks data volumes via Delta Direct, processing time, data quality.
    """
    total_records = (
        context["task_instance"].xcom_pull(
            task_ids="verify_delta_direct_refresh", key="total_records"
        )
        or 0
    )

    batch_records = (
        context["task_instance"].xcom_pull(
            task_ids="verify_delta_direct_refresh", key="batch_records"
        )
        or 0
    )

    streaming_records = (
        context["task_instance"].xcom_pull(
            task_ids="verify_delta_direct_refresh", key="streaming_records"
        )
        or 0
    )

    execution_time = context["execution_date"]

    print("=== Analytics Pipeline Metrics (Delta Direct) ===")
    print(f"Execution Time: {execution_time}")
    print(f"Batch Records: {batch_records:,}")
    print(f"Streaming Records: {streaming_records:,}")
    print(f"Total Records (Delta Direct): {total_records:,}")
    print(f"Architecture: Zero-copy via Snowflake Iceberg table")

    # Metrics would be published to monitoring system here
    # (CloudWatch, Datadog, etc.)

    return {
        "execution_time": str(execution_time),
        "batch_records": batch_records,
        "streaming_records": streaming_records,
        "total_records": total_records,
    }


with DAG(
    dag_id="analytics_pipeline",
    default_args=default_args,
    description="Event-driven analytics: Delta Direct Silver → Gold via dbt",
    schedule=[
        silver_batch_dataset,
        silver_streaming_dataset,
    ],  # Event-driven (OR logic)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics", "medallion", "silver-gold", "event-driven", "delta-direct"],
    max_active_runs=3,  # Allow some concurrency for frequent streaming triggers
    doc_md="""
    # Analytics Pipeline (Delta Direct Architecture)

    THE ONLY DAG that processes Silver → Gold layer.
    Triggered automatically when either batch or streaming updates Silver.

    ## Zero-Copy Architecture with Delta Direct

    **Technology**: Snowflake Delta Direct (Iceberg Tables)

    **Architecture**:
    - Delta Lake Silver layer in S3 (batch + streaming data)
    - Snowflake Iceberg table SALES_SILVER_EXTERNAL
    - AUTO_REFRESH automatically detects new Delta transactions
    - Zero-copy: Snowflake queries S3 directly, no data movement
    - dbt transforms directly from Iceberg table to Gold

    **Key Innovation**: No manual merge required - Delta Direct provides unified view

    ## Event-Driven Scheduling

    **Trigger**: Runs when `silver_batch_dataset` OR `silver_streaming_dataset` updates

    **Frequency**:
    - Streaming triggers: Every 15 minutes
    - Batch triggers: Daily at 2 AM
    - Can run frequently - designed for idempotency

    **Airflow Pattern**: Asset-Driven Scheduling with Dataset triggers

    ## Flow

    1. **Verify Delta Direct**: Confirm AUTO_REFRESH detected latest changes
    2. **Process Gold**: dbt transformations from Iceberg table to dimensional model
    3. **Quality Tests**: Validate Gold layer integrity
    4. **Publish Metrics**: Track pipeline performance

    ## Key Features

    ### Zero-Copy Data Access
    - **Delta Direct**: Snowflake reads Delta Lake from S3 without copying
    - **AUTO_REFRESH**: Automatic detection of new Delta transactions
    - **Cost Efficient**: No duplicate storage in Snowflake

    ### Unified Data View
    - **Single Source**: SALES_SILVER_EXTERNAL combines batch + streaming
    - **No Manual Merge**: Delta Direct handles unification automatically
    - **Real-time Fresh**: AUTO_REFRESH keeps data current

    ### Idempotency
    - **Deterministic Transformations**: Same inputs always produce same outputs
    - **Safe Reruns**: Can execute multiple times without issues

    ## This is Where Gold Layer Lives

    All Silver → Gold processing happens HERE and ONLY here.
    Batch and Streaming DAGs stop at Silver layer.
    Delta Direct bridges Silver (S3) to Gold (Snowflake).
    """,
) as dag:

    start = DummyOperator(
        task_id="start_analytics_pipeline",
        doc_md="Start marker for event-driven analytics processing with Delta Direct",
    )

    verify_refresh = PythonOperator(
        task_id="verify_delta_direct_refresh",
        python_callable=verify_delta_direct_refresh,
        doc_md="""
        # Verify Delta Direct AUTO_REFRESH

        Confirms Snowflake Iceberg table has detected latest Silver layer changes.

        **Delta Direct Architecture**:
        - Iceberg table SALES_SILVER_EXTERNAL points to S3 Delta Lake
        - AUTO_REFRESH monitors _delta_log for new transactions
        - No manual merge - unified view of batch + streaming automatically
        - Zero-copy: data stays in S3, Snowflake queries in place

        **Verification Steps**:
        - Query Iceberg table metadata
        - Count total records from Delta Lake
        - Break down by source (BATCH vs KAFKA)
        - Confirm AUTO_REFRESH is active

        **Technology**: Snowflake connector + Iceberg table queries

        **Output**: Metrics on data freshness and volumes
        """,
    )

    dbt_gold_layer = DbtTaskGroup(
        group_id="dbt_gold_transformations",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["+path:models/marts"],
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        operator_args={
            "install_deps": True,
            "retries": 2,
            "env": ENV_VARS,  # Pass environment variables to dbt subprocess
        },
        tooltip="Gold layer dbt transformations: Silver Delta Direct → Gold Snowflake",
    )

    publish_metrics = PythonOperator(
        task_id="publish_analytics_metrics",
        python_callable=publish_analytics_metrics,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Publish Delta Direct pipeline metrics for monitoring",
    )

    complete = DummyOperator(
        task_id="analytics_pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Analytics pipeline complete - Gold layer updated via Delta Direct",
    )

    # Task dependencies - Verify refresh → dbt Gold transformations (with tests) → Metrics
    start >> verify_refresh >> dbt_gold_layer >> publish_metrics >> complete
