# orchestration/airflow/dags/sales_data_pipeline_dag.py

# ========================================
# ⚠️  MOVED TO LEGACY  ⚠️
# ========================================
# The PostgreSQL-based sales data pipeline has been moved to:
# legacy/orchestration/airflow/dags/legacy_postgres_pipeline_dag.py
#
# This DAG is DEPRECATED and will be removed in Q3 2025.
# Please migrate to the modern cloud-native pipeline:
# - Use: cloud_sales_pipeline_dag.py
# - See: legacy/README.md for migration guide
# ========================================

import warnings
from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# Issue deprecation warning
warnings.warn(
    "The PostgreSQL sales_data_pipeline_dag has been moved to legacy/. "
    "Please use cloud_sales_pipeline_dag.py for new implementations. "
    "See legacy/README.md for migration guide.",
    DeprecationWarning,
    stacklevel=2,
)


def deprecation_notice(**context):
    """Display deprecation notice and migration instructions."""
    message = """
    ========================================
    ⚠️  DEPRECATED DAG EXECUTED  ⚠️
    ========================================

    You are running a deprecated DAG that has been moved to legacy/.

    🔄 IMMEDIATE ACTION REQUIRED:

    1. DISABLE this DAG in the Airflow UI
    2. ENABLE the modern cloud DAG: cloud_sales_pipeline_dag.py
    3. OR use the legacy version: legacy_postgres_sales_aggregation

    📚 Migration Resources:
    - Migration Guide: legacy/README.md
    - Modern Architecture: docs/architecture/modern-pipeline.md
    - Support: Create GitHub issue with 'migration-support' tag

    ⏰ Timeline:
    - Q1 2025: Components moved to legacy (CURRENT)
    - Q2 2025: Deprecation warnings added
    - Q3 2025: Legacy components removed

    ========================================
    """
    print(message)
    return message


default_args = {
    "owner": ["system"],
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # Don't retry deprecation notices
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="sales_data_aggregation",
    description="🚨 MOVED TO LEGACY - Use cloud_sales_pipeline_dag.py instead",
    default_args=default_args,
    schedule_interval=None,  # Disabled to prevent accidental runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["deprecated", "moved-to-legacy", "use-cloud-dag-instead"],
    doc_md="""
    # 🚨 DAG MOVED TO LEGACY

    This PostgreSQL-based DAG has been **moved to legacy** and is deprecated.

    ## ⚡ Quick Migration

    ### For New Users:
    ```
    Use: cloud_sales_pipeline_dag.py
    ```

    ### For Existing PostgreSQL Users:
    ```
    Use: legacy_postgres_sales_aggregation
    Location: legacy/orchestration/airflow/dags/
    ```

    ## 📋 Migration Checklist

    - [ ] Disable this DAG in Airflow UI
    - [ ] Choose modern (cloud_sales_pipeline_dag.py) or legacy version
    - [ ] Update environment configuration
    - [ ] Test new pipeline with sample data
    - [ ] Update monitoring and alerting
    - [ ] Plan data migration (if switching from PostgreSQL)

    ## 🔗 Resources

    - **Migration Guide**: [legacy/README.md](../../../legacy/README.md)
    - **Modern Architecture**: [docs/architecture/](../../../docs/architecture/)
    - **Troubleshooting**: [docs/migration/troubleshooting.md](../../../docs/migration/troubleshooting.md)

    ## 🆘 Support

    Create a GitHub issue with tag `migration-support` for assistance.
    """,
) as dag:

    # Single task that displays deprecation notice
    show_deprecation_notice = PythonOperator(
        task_id="show_deprecation_notice",
        python_callable=deprecation_notice,
        doc_md="""
        Displays deprecation notice and migration instructions.
        This DAG should not be used for actual data processing.
        """,
    )

    show_deprecation_notice
