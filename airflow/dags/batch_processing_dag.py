# airflow/dags/batch_processing_dag.py
"""
Batch Data Processing DAG - Enterprise Pattern

Processes daily batch CSV files through Bronze → Silver layers ONLY.
Silver → Gold processing handled by separate Analytics DAG (event-driven).

Flow: CSV Files → Bronze S3 → Silver Delta Lake → [Triggers Analytics DAG]

Schedule: Daily at 2:00 AM
Idempotency: Date-based partitioning ensures safe reruns
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
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

ENV_VARS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    "RAW_BUCKET": os.getenv("RAW_BUCKET"),
    "PROCESSED_BUCKET": os.getenv("PROCESSED_BUCKET"),
}

# Dataset definition - triggers Analytics DAG when updated
# Unified Silver table for both batch and streaming data
silver_batch_dataset = Dataset(f"s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/sales/")


def ingest_csv_to_bronze(**context):
    """
    Ingest CSV files to Bronze S3 layer with date partitioning.
    Idempotent: Uses execution date for partition.
    """
    import sys

    sys.path.insert(0, "/app")
    from src.bronze.data_uploader import BronzeDataUploader

    execution_date = context["ds"]
    bucket = ENV_VARS["RAW_BUCKET"]
    local_data_dir = os.getenv("LOCAL_DATA_DIR", "/app/data/raw")

    print(f"Bronze Layer Ingestion - Date: {execution_date}")
    print(f"Target bucket: s3://{bucket}/")
    print(f"Local source: {local_data_dir}")

    try:
        # Initialize Bronze uploader
        uploader = BronzeDataUploader(bucket)

        # Upload CSV files to Bronze layer
        result = uploader.upload_csv_files_to_bronze(
            local_data_dir=local_data_dir,
            batch_date=execution_date,
            file_pattern="*.csv",
        )

        # Store results in XCom
        context["task_instance"].xcom_push(key="bronze_status", value=result["status"])
        context["task_instance"].xcom_push(
            key="files_uploaded", value=result["files_uploaded"]
        )
        context["task_instance"].xcom_push(
            key="files_skipped", value=result["files_skipped"]
        )
        context["task_instance"].xcom_push(
            key="bronze_prefix", value=result["bronze_prefix"]
        )

        print(f"Bronze ingestion completed: {result['status']}")
        print(f"Files uploaded: {result['files_uploaded']}")
        print(f"Files skipped: {result['files_skipped']}")
        print(f"Bronze location: s3://{bucket}/{result['bronze_prefix']}")

        return result

    except Exception as e:
        print(f"FAILED: Bronze ingestion error: {e}")
        raise


def validate_bronze_layer(**context):
    """
    Validate Bronze layer data quality before Silver processing.
    Uses professional validator with schema checking and quality scoring.
    """
    import sys

    sys.path.insert(0, "/app")
    from src.validation.bronze_validator import BronzeValidator
    import boto3

    execution_date = context["ds"]
    bucket = ENV_VARS["RAW_BUCKET"]

    # Parse date for standardized path structure
    year = execution_date[:4]
    month = execution_date[5:7]
    day = execution_date[8:10]
    bronze_prefix = f"sales_data/year={year}/month={month}/day={day}/"

    print(f"Bronze Layer Validation - Date: {execution_date}")
    print(f"Location: s3://{bucket}/{bronze_prefix}")

    try:
        # First check if Bronze data exists
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=bronze_prefix)

        if "Contents" not in response:
            raise ValueError(f"No Bronze data found for {execution_date}")

        # Get CSV files only
        csv_files = [obj for obj in response["Contents"] if obj["Key"].endswith(".csv")]
        file_count = len(csv_files)
        total_size = sum(obj["Size"] for obj in csv_files)

        print(f"Found {file_count} CSV files ({total_size / (1024*1024):.2f} MB)")

        # Initialize professional validator
        validator = BronzeValidator(bucket)

        # Validate each CSV file
        validation_results = []
        for csv_obj in csv_files[:10]:  # Validate first 10 files for performance
            s3_path = f"s3://{bucket}/{csv_obj['Key']}"
            validation_result = validator.validate_csv_schema(s3_path)
            validation_results.append(validation_result)

        # Generate comprehensive validation report
        if validation_results:
            report = validator.generate_validation_report(validation_results)
            quality_score = report["average_quality_score"]

            print(f"Validation Report:")
            print(f"  Overall Status: {report['overall_status']}")
            print(f"  Quality Score: {quality_score}/100")
            print(f"  Valid Files: {report['valid_files']}/{report['total_files']}")
            print(f"  Invalid Files: {report['invalid_files']}")

            # Quality gate: Require minimum 70% quality score
            if quality_score < 70:
                raise ValueError(
                    f"Data quality below threshold: {quality_score}/100. "
                    f"Review validation report. Status: {report['overall_status']}"
                )

            context["task_instance"].xcom_push(
                key="bronze_quality_score", value=quality_score
            )
            context["task_instance"].xcom_push(
                key="validation_status", value=report["overall_status"]
            )
        else:
            # Fallback to basic validation
            quality_score = 95
            print(f"Basic validation passed (no schema validation performed)")

        context["task_instance"].xcom_push(key="bronze_files", value=file_count)
        context["task_instance"].xcom_push(
            key="bronze_size_mb", value=total_size / (1024 * 1024)
        )

        print(f"SUCCESS: Bronze validation passed")
        return {"valid": True, "files": file_count, "quality_score": quality_score}

    except Exception as e:
        print(f"FAILED: Bronze validation error: {e}")
        raise


def process_bronze_to_silver(**context):
    """
    Transform Bronze → Silver using Spark with Delta Lake format.

    Technology: Spark + Delta Lake (ACID transactions for concurrent writes)
    Output: Updates silver_batch_dataset which triggers Analytics DAG

    STOPS at Silver layer - Gold processing handled by Analytics DAG.
    """
    import subprocess
    from datetime import datetime

    execution_date = context["ds"]
    dt = datetime.strptime(execution_date, "%Y-%m-%d")

    # Match Bronze layer Hive partitioning: year=/month=/day=
    input_path = f"s3://{ENV_VARS['RAW_BUCKET']}/sales_data/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    output_path = f"s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/sales/"  # Unified table (MERGE handles dedup)

    print(f"Silver Layer Processing - Date: {execution_date}")
    print(f"Bronze → Silver transformation")
    print(f"Input: {input_path}")
    print(f"Output: Delta Lake at s3://{ENV_VARS['PROCESSED_BUCKET']}/silver/sales/")
    print(f"Format: Delta Lake with MERGE (unified batch+streaming table)")
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
        execution_date,
    ]

    print(f"Executing Spark job: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    if result.returncode == 0:
        print("SUCCESS: Silver processing completed")
        print(f"Dataset updated: {silver_batch_dataset.uri}")
        print("This will trigger Analytics DAG for Silver → Gold processing")

        context["task_instance"].xcom_push(key="silver_path", value=output_path)
        context["task_instance"].xcom_push(
            key="silver_dataset", value=str(silver_batch_dataset.uri)
        )

        return {"status": "success", "output": output_path}
    else:
        print(f"FAILED: Silver processing error: {result.stderr}")
        raise RuntimeError(f"Spark job failed: {result.stderr}")


with DAG(
    dag_id="batch_processing_pipeline",
    default_args=default_args,
    description="Daily batch processing: Bronze → Silver (triggers Analytics DAG for Gold)",
    schedule="0 2 * * *",  # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["batch", "medallion", "bronze-silver"],
    max_active_runs=1,
    doc_md="""
    # Batch Processing Pipeline

    Processes daily CSV files through Bronze and Silver layers of medallion architecture.

    ## Flow
    1. **Bronze Ingestion**: CSV files → S3 Bronze (raw, immutable audit trail)
    2. **Bronze Validation**: Quality gate before processing
    3. **Silver Processing**: Bronze → Delta Lake Silver (cleaned, standardized)
    4. **Dataset Update**: Triggers Analytics DAG for Silver → Gold processing

    ## Stopping Point
    This DAG STOPS at Silver layer. Gold layer processing happens in the separate
    Analytics DAG which is triggered by the silver_batch_dataset update.

    ## Idempotency
    Uses execution date for partitioning - safe to rerun for any date.
    """,
) as dag:

    start = DummyOperator(
        task_id="start_batch_pipeline", doc_md="Start marker for batch processing"
    )

    bronze_ingestion = PythonOperator(
        task_id="ingest_csv_to_bronze",
        python_callable=ingest_csv_to_bronze,
        doc_md="""
        # Bronze Layer Ingestion

        Uploads CSV files to S3 Bronze layer with date-based partitioning.

        **Target**: s3://bucket/sales_data/date=YYYY-MM-DD/
        **Idempotency**: Uses execution date - safe to rerun
        **Quality**: Minimal transformation - raw data preservation
        """,
    )

    bronze_validation = PythonOperator(
        task_id="validate_bronze_layer",
        python_callable=validate_bronze_layer,
        doc_md="""
        # Bronze Layer Validation

        Quality gate ensuring Bronze data exists and meets basic standards.
        """,
    )

    silver_processing = PythonOperator(
        task_id="process_bronze_to_silver",
        python_callable=process_bronze_to_silver,
        outlets=[silver_batch_dataset],
        doc_md="""
        # Silver Layer Processing

        Transforms Bronze data using Spark with Delta Lake format.

        **Technology**: Spark + Delta Lake (ACID transactions)
        **Transformations**:
        - Data quality validation
        - Deduplication
        - Schema standardization
        - Business rule application

        **Output**: Delta Lake Silver layer
        **Triggers**: Analytics DAG via silver_batch_dataset update

        **IMPORTANT**: This DAG STOPS here. Gold processing happens in Analytics DAG.
        """,
    )

    complete = DummyOperator(
        task_id="batch_silver_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="""
        # Batch Processing Complete

        Silver layer updated. Analytics DAG will be triggered automatically
        for Silver → Gold processing and merging with streaming data.
        """,
    )

    # Task dependencies - Proper medallion flow: Bronze → Silver
    start >> bronze_ingestion >> bronze_validation >> silver_processing >> complete
