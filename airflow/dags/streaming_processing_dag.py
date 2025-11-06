# airflow/dags/streaming_processing_dag.py
"""
Streaming Data Processing DAG - Enterprise Pattern

Processes streaming data from Bronze → Silver layers ONLY.
Reads data that Kafka Connect already wrote to Bronze S3, processes with Spark.

Flow: Bronze S3 (Kafka Connect output) → Spark ETL → Silver Delta Lake → [Triggers Analytics DAG]

Schedule: Every 15 minutes (micro-batch pattern)
Technology: Spark Batch (not Spark Streaming) reading from S3
Idempotency: Time-window based processing ensures safe reruns
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.datasets import Dataset

# Configuration
default_args = {
    "owner": os.getenv("OWNER_NAME", "data-engineering"),
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "data-team@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

ENV_VARS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    "RAW_BUCKET": os.getenv("RAW_BUCKET"),
    "PROCESSED_BUCKET": os.getenv("PROCESSED_BUCKET"),
}

# Dataset definition - triggers Analytics DAG when updated
silver_streaming_dataset = Dataset(
    f"s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/streaming/"
)


def validate_bronze_streaming_data(**context):
    """
    Validate streaming data in Bronze S3 layer.
    Checks for new data written by Kafka Connect in the last time window.
    """
    import boto3
    from datetime import datetime, timedelta

    bucket = ENV_VARS["RAW_BUCKET"]
    prefix = "sales_data/"  # Kafka Connect adds year/month/day/hour partitions

    # Time window for this micro-batch (last 15 minutes)
    execution_time = context["execution_date"]
    start_time = execution_time - timedelta(minutes=15)

    print(f"Bronze Streaming Validation - Time Window")
    print(f"  Start: {start_time}")
    print(f"  End: {execution_time}")
    print(f"  Location: s3://{bucket}/{prefix}")

    s3_client = boto3.client("s3")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)

        if "Contents" not in response:
            print("WARNING: No streaming data found in Bronze layer")
            context["task_instance"].xcom_push(key="has_new_data", value=False)
            return {"has_data": False}

        # Filter objects by time window
        new_objects = [
            obj
            for obj in response["Contents"]
            if obj["LastModified"].replace(tzinfo=None)
            >= start_time.replace(tzinfo=None)
            and obj["LastModified"].replace(tzinfo=None)
            < execution_time.replace(tzinfo=None)
        ]

        new_count = len(new_objects)
        total_size = sum(obj["Size"] for obj in new_objects)

        print(f"New objects in window: {new_count}")
        print(f"Total size: {total_size / (1024*1024):.2f} MB")

        context["task_instance"].xcom_push(key="has_new_data", value=new_count > 0)
        context["task_instance"].xcom_push(key="new_files", value=new_count)
        context["task_instance"].xcom_push(
            key="size_mb", value=total_size / (1024 * 1024)
        )

        if new_count > 0:
            print("SUCCESS: New streaming data available for processing")
            return {"has_data": True, "files": new_count}
        else:
            print("INFO: No new streaming data in this window - skipping processing")
            return {"has_data": False}

    except Exception as e:
        print(f"FAILED: Bronze validation error: {e}")
        raise


def process_streaming_bronze_to_silver(**context):
    """
    Transform streaming Bronze → Silver using Spark with Delta Lake.

    Key Architectural Decision:
    - Uses Spark BATCH (not Spark Streaming) to read from S3
    - Reads data that Kafka Connect already wrote to Bronze
    - Processes in micro-batches (15-minute windows)
    - Delta Lake handles concurrent writes with batch DAG

    STOPS at Silver layer - Gold processing handled by Analytics DAG.
    """
    import subprocess

    # Check if there's new data to process
    has_new_data = context["task_instance"].xcom_pull(
        task_ids="validate_bronze_streaming_data", key="has_new_data"
    )

    if not has_new_data:
        print("No new data to process - skipping Silver processing")
        return {"status": "skipped", "reason": "no_new_data"}

    execution_time = context["execution_date"]
    start_time = execution_time - timedelta(minutes=15)

    # Get execution date parts for path
    dt = execution_time

    # Input: Bronze S3 (written by Kafka Connect with Hive partitioning)
    # Kafka Connect writes to: sales_data/sales_events/year=YYYY/month=MM/day=DD/hour=HH/
    input_path = f"s3://{ENV_VARS['RAW_BUCKET']}/sales_data/sales_events/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/*/"

    # Output: Unified Silver Delta Lake (merged with batch data)
    output_path = f"s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/sales/"

    print(f"Silver Layer Processing - Streaming Micro-Batch")
    print(f"Bronze → Silver transformation")
    print(f"Input: {input_path}")
    print(f"Output: {output_path} (unified batch+streaming table)")
    print(f"Time window: {start_time} to {execution_time}")
    print(f"Technology: Spark Batch + Delta Lake MERGE (ACID for concurrent writes)")
    print(f"Deduplication: MERGE on order_id business key")

    # Execute Spark job for Silver processing
    # Note: batch_etl.py handles Delta output internally using PROCESSED_BUCKET env var
    cmd = [
        "docker",
        "exec",
        "sales_data_aggregation_pipeline-spark-local-1",
        "python3",
        "/app/src/spark/jobs/batch_etl.py",
        "--input-path",
        input_path,
        "--batch-id",
        f"streaming-{execution_time.strftime('%Y%m%d%H%M')}",
    ]

    print(f"Executing Spark job: {' '.join(cmd)}")

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=1200  # 20 minutes timeout
    )

    if result.returncode == 0:
        print("SUCCESS: Silver processing completed")
        print(f"Dataset updated: {silver_streaming_dataset.uri}")
        print("This will trigger Analytics DAG for Silver → Gold processing")

        context["task_instance"].xcom_push(key="silver_path", value=output_path)
        context["task_instance"].xcom_push(
            key="silver_dataset", value=str(silver_streaming_dataset.uri)
        )

        return {"status": "success", "output": output_path}
    else:
        print(f"FAILED: Silver processing error: {result.stderr}")
        raise RuntimeError(f"Spark job failed: {result.stderr}")


with DAG(
    dag_id="streaming_processing_pipeline",
    default_args=default_args,
    description="Streaming processing: Bronze → Silver micro-batches (triggers Analytics DAG for Gold)",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["streaming", "medallion", "bronze-silver", "micro-batch"],
    max_active_runs=1,
    doc_md="""
    # Streaming Processing Pipeline

    Processes streaming data in micro-batches from Bronze to Silver layers.

    ## Architecture Decision (Research-Based)

    **Pattern Used**: Kafka → Kafka Connect S3 Sink → Bronze S3 → Spark Batch → Silver Delta Lake

    **Why NOT Spark Streaming directly from Kafka?**
    - Bronze layer provides immutable audit trail (compliance requirement)
    - Same transformation logic as batch processing (code reuse)
    - Kafka Connect provides exactly-once semantics to S3
    - Simpler operational model (one Spark codebase)

    ## Flow
    1. **Bronze Validation**: Check for new streaming data (15-minute window)
    2. **Silver Processing**: Spark Batch reads S3, transforms to Delta Lake
    3. **Dataset Update**: Triggers Analytics DAG for Silver → Gold processing

    ## Stopping Point
    This DAG STOPS at Silver layer. Gold layer processing happens in the separate
    Analytics DAG which is triggered by the silver_streaming_dataset update.

    ## Concurrency
    Delta Lake ACID transactions handle concurrent writes with Batch Processing DAG.
    No manual coordination needed - automatic conflict resolution.

    ## Idempotency
    Time-window based processing - safe to rerun for any time period.

    ## Schedule
    Runs every 15 minutes for near real-time processing.
    """,
) as dag:

    start = DummyOperator(
        task_id="start_streaming_processing",
        doc_md="Start marker for streaming processing micro-batch",
    )

    bronze_validation = PythonOperator(
        task_id="validate_bronze_streaming_data",
        python_callable=validate_bronze_streaming_data,
        doc_md="""
        # Bronze Streaming Data Validation

        Checks for new streaming data in Bronze S3 layer for current time window.

        **Source**: Kafka Connect S3 Sink output
        **Location**: s3://bucket/sales_transactions/streaming_data/
        **Window**: Last 15 minutes
        """,
    )

    silver_processing = PythonOperator(
        task_id="process_streaming_bronze_to_silver",
        python_callable=process_streaming_bronze_to_silver,
        outlets=[silver_streaming_dataset],
        doc_md="""
        # Silver Layer Processing (Streaming Micro-Batch)

        Transforms streaming Bronze data using Spark Batch with Delta Lake format.

        **Technology**: Spark Batch (not Spark Streaming) + Delta Lake

        **Why Spark Batch?**
        - Reads from S3 (where Kafka Connect wrote data)
        - Same transformation logic as batch processing
        - Code reuse and operational simplicity

        **Transformations**:
        - Data quality validation
        - Schema standardization
        - Deduplication (within window)
        - Business rule application

        **Concurrency**: Delta Lake ACID handles concurrent writes with Batch DAG

        **Output**: Delta Lake Silver layer (append mode)
        **Triggers**: Analytics DAG via silver_streaming_dataset update

        **IMPORTANT**: This DAG STOPS here. Gold processing happens in Analytics DAG.
        """,
    )

    complete = DummyOperator(
        task_id="streaming_silver_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="""
        # Streaming Processing Complete

        Silver layer updated with streaming micro-batch.
        Analytics DAG will be triggered automatically for Silver → Gold processing.
        """,
    )

    # Task dependencies - Proper medallion flow: Bronze → Silver
    start >> bronze_validation >> silver_processing >> complete
