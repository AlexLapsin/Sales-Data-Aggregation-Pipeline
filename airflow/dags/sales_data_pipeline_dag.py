from datetime import timedelta, datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Dynamically resolve repo paths
REPO_ROOT = Path(__file__).parents[2].resolve()
DATA_DIR  = str(REPO_ROOT / "data")
DB_DIR    = str(REPO_ROOT / "db")
OUT_DIR   = str(REPO_ROOT / "output")

# Default args for all tasks
default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'email': ['alex.lapsin.dev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='sales_data_aggregation',
    default_args=default_args,
    description='Orchestrates the sales data ETL pipeline using DockerOperator',
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 25),
    catchup=False,
    tags=['etl', 'sales'],
) as dag:

    # Task: Create database tables
    create_tables = DockerOperator(
        task_id='create_tables',
        image='sales-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        command='python -m scripts.create_tables',
        mounts=[
            Mount(source=DATA_DIR, target='/app/data', type='bind'),
            Mount(source=OUT_DIR, target='/app/output', type='bind'),
            Mount(source=DB_DIR,   target='/app/db',   type='bind'),
        ],
        working_dir='/app/src',
    )

    # Task: Extract data
    extract = DockerOperator(
        task_id='extract',
        image='sales-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        command='python -m scripts.run_extract',
        mounts=[
            Mount(source=DATA_DIR, target='/app/data', type='bind'),
            Mount(source=OUT_DIR, target='/app/output', type='bind'),
            Mount(source=DB_DIR,   target='/app/db',   type='bind'),
        ],
        working_dir='/app/src',
    )

    # Task: Transform data
    transform = DockerOperator(
        task_id='transform',
        image='sales-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        command='python -m scripts.run_transform',
        mounts=[
            Mount(source=DATA_DIR, target='/app/data', type='bind'),
            Mount(source=OUT_DIR, target='/app/output', type='bind'),
            Mount(source=DB_DIR,   target='/app/db',   type='bind'),
        ],
        working_dir='/app/src',
    )

    # Task: Load data
    load = DockerOperator(
        task_id='load',
        image='sales-pipeline:latest',
        api_version='auto',
        auto_remove=True,
        command='python -m scripts.run_load',
        mounts=[
            Mount(source=DATA_DIR, target='/app/data', type='bind'),
            Mount(source=OUT_DIR, target='/app/output', type='bind'),
            Mount(source=DB_DIR,   target='/app/db',   type='bind'),
        ],
        working_dir='/app/src',
    )

    # Define task order
    create_tables >> extract >> transform >> load
