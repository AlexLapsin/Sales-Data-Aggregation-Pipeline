# airflow/dags/sales_data_pipeline_dag.py
from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------- Default args ----------------
default_args = {
    "owner": "alex",
    "depends_on_past": False,
    "email": ["alex.lapsin.dev@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    # Optional:
    # "execution_timeout": timedelta(minutes=30),
}

# ---------------- Environment for tasks ----------------
ENV_KEYS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "S3_BUCKET",  # raw bucket
    "PROCESSED_BUCKET",  # processed bucket
    "S3_PREFIX",  # will be overridden per-run below to runs/{{ ds }}/
    "RDS_HOST",
    "RDS_PORT",
    "RDS_DB",
    "RDS_USER",
    "RDS_PASS",
    "SALES_THRESHOLD",
]
ENV_VARS = {k: v for k, v in ((k, os.getenv(k)) for k in ENV_KEYS) if v}

IMAGE = os.getenv("PIPELINE_IMAGE", "sales-pipeline:latest")
HOST_DATA_DIR = os.getenv(
    "HOST_DATA_DIR", "/opt/data"
)  # absolute host path; must contain ./raw/*.csv

with DAG(
    dag_id="sales_data_aggregation",
    description="Sales ETL using DockerOperator (S3-first design).",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 7, 25),
    catchup=False,
    tags=["etl", "sales"],
) as dag:

    # 1) Preflight: verify S3 + RDS connectivity
    preflight_check = DockerOperator(
        task_id="preflight_check",
        image=IMAGE,
        api_version="auto",
        auto_remove=True,
        command="python -m scripts.preflight_check",
        working_dir="/app/src",
        environment=ENV_VARS,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # 2) Stage raw: upload local CSVs from host data/raw -> S3 raw
    stage_raw_to_s3 = DockerOperator(
        task_id="stage_raw_to_s3",
        image=IMAGE,
        api_version="auto",
        auto_remove=True,
        command="python -m scripts.upload_data",
        working_dir="/app/src",
        environment={
            **ENV_VARS,
            # per-run partition, e.g. runs/2025-08-19/
            "S3_PREFIX": "runs/{{ ds }}/",
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=HOST_DATA_DIR, target="/app/data", type="bind")],
    )

    # 3) Transform and write Parquet to S3 processed
    transform = DockerOperator(
        task_id="transform",
        image=IMAGE,
        api_version="auto",
        auto_remove=True,
        command="python -m scripts.run_transform",
        working_dir="/app/src",
        environment={
            **ENV_VARS,
            "S3_PREFIX": "runs/{{ ds }}/",
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # 4) Create tables in RDS (schema) before loading
    create_tables = DockerOperator(
        task_id="create_tables",
        image=IMAGE,
        api_version="auto",
        auto_remove=True,
        command="python -m scripts.create_tables",
        working_dir="/app/src",
        environment=ENV_VARS,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # 5) Load: read Parquet from S3 processed -> load into RDS (no volumes)
    load = DockerOperator(
        task_id="load",
        image=IMAGE,
        api_version="auto",
        auto_remove=True,
        command="python -m scripts.run_load",
        working_dir="/app/src",
        environment={
            **ENV_VARS,
            "S3_PREFIX": "runs/{{ ds }}/",
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
    )

    # Flow: 1 → 2 → 3 → 4 → 5
    preflight_check >> stage_raw_to_s3 >> transform >> create_tables >> load
