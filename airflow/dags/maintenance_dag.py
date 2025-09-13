# airflow/dags/maintenance_dag.py
"""
Pipeline Maintenance DAG
Handles routine maintenance tasks for the cloud sales pipeline:
1. Data cleanup and archival
2. Performance optimization
3. Schema maintenance
4. Log cleanup
5. Resource management
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------- Configuration ----------------
default_args = {
    "owner": os.getenv("OWNER_NAME", "data-engineering"),
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "data-team@company.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

ENV_VARS = {
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "S3_BUCKET": os.getenv("S3_BUCKET"),
}


# ---------------- Helper Functions ----------------
def calculate_table_sizes(**context):
    """Calculate table sizes and recommend optimization actions"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default",
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
    )

    # Get table sizes and statistics
    table_stats_query = """
    SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        ROW_COUNT,
        BYTES,
        BYTES / (1024*1024*1024) as GB,
        CLUSTERING_KEY,
        AUTO_CLUSTERING_ON
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('RAW', 'STAGING', 'MARTS')
        AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY BYTES DESC
    """

    try:
        results = hook.get_records(table_stats_query)

        recommendations = []
        for row in results:
            (
                schema,
                table,
                row_count,
                bytes_size,
                gb_size,
                clustering_key,
                auto_clustering,
            ) = row

            # Recommend clustering for large tables without it
            if gb_size > 1 and clustering_key is None:
                recommendations.append(
                    {
                        "table": f"{schema}.{table}",
                        "action": "add_clustering",
                        "reason": f"Large table ({gb_size:.2f} GB) without clustering",
                    }
                )

            # Recommend vacuum for very large tables
            if gb_size > 10:
                recommendations.append(
                    {
                        "table": f"{schema}.{table}",
                        "action": "vacuum",
                        "reason": f"Very large table ({gb_size:.2f} GB) may benefit from maintenance",
                    }
                )

        context["task_instance"].xcom_push(
            key="table_recommendations", value=recommendations
        )
        return recommendations

    except Exception as e:
        error_msg = f"Failed to calculate table sizes: {str(e)}"
        context["task_instance"].xcom_push(
            key="table_recommendations", value=[{"error": error_msg}]
        )
        raise


# ---------------- DAG Definition ----------------
with DAG(
    dag_id="pipeline_maintenance",
    description="Routine maintenance tasks for the cloud sales pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * 0",  # Run weekly on Sunday at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance", "cleanup", "optimization"],
    max_active_runs=1,
) as dag:

    start_maintenance = DummyOperator(
        task_id="start_maintenance",
        doc_md="Starts the weekly pipeline maintenance process",
    )

    # ---------------- Data Cleanup ----------------
    cleanup_old_raw_data = SnowflakeOperator(
        task_id="cleanup_old_raw_data",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Archive data older than 90 days to a backup table
        CREATE OR REPLACE TABLE SALES_DW.RAW.SALES_RAW_ARCHIVE AS
        SELECT * FROM SALES_DW.RAW.SALES_RAW
        WHERE PARTITION_DATE < CURRENT_DATE() - INTERVAL '90 days';

        -- Delete old data from main table
        DELETE FROM SALES_DW.RAW.SALES_RAW
        WHERE PARTITION_DATE < CURRENT_DATE() - INTERVAL '90 days';

        -- Do the same for batch data
        CREATE OR REPLACE TABLE SALES_DW.RAW.SALES_BATCH_RAW_ARCHIVE AS
        SELECT * FROM SALES_DW.RAW.SALES_BATCH_RAW
        WHERE PARTITION_DATE < CURRENT_DATE() - INTERVAL '90 days';

        DELETE FROM SALES_DW.RAW.SALES_BATCH_RAW
        WHERE PARTITION_DATE < CURRENT_DATE() - INTERVAL '90 days';
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="RAW",
        doc_md="Archives and removes data older than 90 days from raw tables",
    )

    cleanup_old_staging_data = SnowflakeOperator(
        task_id="cleanup_old_staging_data",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Clean up old staging data (views don't store data, but clean temp tables)
        DROP TABLE IF EXISTS SALES_DW.STAGING.TEMP_STAGING_DATA;
        DROP TABLE IF EXISTS SALES_DW.STAGING.TEMP_PROCESSING;

        -- Clean up any materialized staging tables older than 30 days
        -- This would need to be customized based on actual staging table structure
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="STAGING",
        doc_md="Cleans up temporary staging tables and old processing artifacts",
    )

    # ---------------- S3 Data Cleanup ----------------
    cleanup_s3_old_data = DockerOperator(
        task_id="cleanup_s3_old_data",
        image="amazon/aws-cli:latest",
        command=[
            "bash",
            "-c",
            f"""
            # List objects older than 90 days
            aws s3api list-objects-v2 \
                --bucket {ENV_VARS['S3_BUCKET']} \
                --query 'Contents[?LastModified<`$(date -d "90 days ago" -u +"%Y-%m-%dT%H:%M:%S.000Z")`].[Key]' \
                --output text > /tmp/old_files.txt

            # Delete old files if any exist
            if [ -s /tmp/old_files.txt ]; then
                echo "Deleting old S3 files:"
                cat /tmp/old_files.txt
                aws s3 rm s3://{ENV_VARS['S3_BUCKET']} --recursive --exclude "*" --include "$(cat /tmp/old_files.txt | tr '\n' ' ' | sed 's/ / --include /g')"
            else
                echo "No old files to delete"
            fi
            """,
        ],
        environment=ENV_VARS,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        doc_md="Removes S3 objects older than 90 days to manage storage costs",
    )

    # ---------------- Performance Optimization ----------------
    calculate_table_stats = PythonOperator(
        task_id="calculate_table_stats",
        python_callable=calculate_table_sizes,
        doc_md="Analyzes table sizes and generates optimization recommendations",
    )

    optimize_table_clustering = SnowflakeOperator(
        task_id="optimize_table_clustering",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Recalculate clustering for fact tables
        ALTER TABLE SALES_DW.MARTS.FACT_SALES RECLUSTER;
        ALTER TABLE SALES_DW.MARTS.FACT_SALES_DAILY RECLUSTER;

        -- Update table statistics
        ANALYZE TABLE SALES_DW.MARTS.FACT_SALES COMPUTE STATISTICS;
        ANALYZE TABLE SALES_DW.MARTS.FACT_SALES_DAILY COMPUTE STATISTICS;
        ANALYZE TABLE SALES_DW.MARTS.DIM_PRODUCT COMPUTE STATISTICS;
        ANALYZE TABLE SALES_DW.MARTS.DIM_STORE COMPUTE STATISTICS;
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="MARTS",
        doc_md="Reclusters tables and updates statistics for optimal performance",
    )

    vacuum_tables = SnowflakeOperator(
        task_id="vacuum_tables",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Vacuum large tables to reclaim space
        ALTER TABLE SALES_DW.MARTS.FACT_SALES COMPACT;
        ALTER TABLE SALES_DW.MARTS.FACT_SALES_DAILY COMPACT;

        -- Clean up deleted data
        ALTER TABLE SALES_DW.RAW.SALES_RAW PURGE;
        ALTER TABLE SALES_DW.RAW.SALES_BATCH_RAW PURGE;
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        doc_md="Compacts tables and purges deleted data to reclaim storage",
    )

    # ---------------- Schema Maintenance ----------------
    update_schema_documentation = DockerOperator(
        task_id="update_schema_documentation",
        image=os.getenv("PIPELINE_IMAGE", "sales-pipeline:latest"),
        command='bash -c "cd /app/dbt && dbt docs generate --profiles-dir /app/dbt"',
        environment=ENV_VARS,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        mounts=[
            {"source": os.path.abspath("./dbt"), "target": "/app/dbt", "type": "bind"}
        ],
        doc_md="Regenerates dbt documentation with latest schema changes",
    )

    validate_schema_integrity = SnowflakeOperator(
        task_id="validate_schema_integrity",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Check for orphaned records in fact tables
        SELECT
            'orphaned_products' as check_type,
            COUNT(*) as orphan_count
        FROM SALES_DW.MARTS.FACT_SALES f
        LEFT JOIN SALES_DW.MARTS.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        WHERE p.PRODUCT_KEY IS NULL

        UNION ALL

        SELECT
            'orphaned_stores' as check_type,
            COUNT(*) as orphan_count
        FROM SALES_DW.MARTS.FACT_SALES f
        LEFT JOIN SALES_DW.MARTS.DIM_STORE s ON f.STORE_KEY = s.STORE_KEY
        WHERE s.STORE_KEY IS NULL

        UNION ALL

        SELECT
            'orphaned_dates' as check_type,
            COUNT(*) as orphan_count
        FROM SALES_DW.MARTS.FACT_SALES f
        LEFT JOIN SALES_DW.MARTS.DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
        WHERE d.DATE_KEY IS NULL
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="MARTS",
        doc_md="Validates referential integrity across dimensional model",
    )

    # ---------------- Log and Temp Cleanup ----------------
    cleanup_airflow_logs = BashOperator(
        task_id="cleanup_airflow_logs",
        bash_command="""
        # Clean up Airflow logs older than 30 days
        find /opt/airflow/logs -name "*.log" -mtime +30 -delete || true
        find /opt/airflow/logs -type d -empty -delete || true

        # Clean up temporary files
        find /tmp -name "*sales_pipeline*" -mtime +7 -delete || true

        echo "Log cleanup completed"
        """,
        doc_md="Removes old Airflow logs and temporary files",
    )

    cleanup_docker_resources = BashOperator(
        task_id="cleanup_docker_resources",
        bash_command="""
        # Clean up unused Docker images and containers
        docker system prune -f || true
        docker image prune -f || true
        docker container prune -f || true

        # Clean up old pipeline images (keep latest 3)
        docker images sales-pipeline --format "table {{.Repository}}:{{.Tag}}\t{{.CreatedAt}}" | \
        tail -n +2 | sort -k2 -r | tail -n +4 | awk '{print $1}' | \
        xargs -r docker rmi || true

        echo "Docker cleanup completed"
        """,
        doc_md="Cleans up unused Docker resources and old pipeline images",
    )

    # ---------------- Kafka Maintenance ----------------
    kafka_log_cleanup = DockerOperator(
        task_id="kafka_log_cleanup",
        image="confluentinc/cp-kafka:latest",
        command=[
            "bash",
            "-c",
            f"""
            # Clean up old Kafka logs (this would typically be done on Kafka brokers)
            echo "Kafka log cleanup would be performed here"
            echo "In production, this would connect to Kafka brokers and clean old segments"

            # Check topic sizes
            kafka-log-dirs --bootstrap-server {ENV_VARS.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')} \
                --describe --json | jq '.brokers[].logDirs[].partitions[]' || true
            """,
        ],
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Maintains Kafka logs and checks topic sizes",
    )

    # ---------------- Resource Optimization ----------------
    warehouse_scaling_check = SnowflakeOperator(
        task_id="warehouse_scaling_check",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Check warehouse usage over the past week
        SELECT
            DATE(START_TIME) as usage_date,
            WAREHOUSE_NAME,
            COUNT(*) as query_count,
            AVG(EXECUTION_TIME) as avg_execution_time_ms,
            SUM(CREDITS_USED) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= CURRENT_DATE() - INTERVAL '7 days'
            AND WAREHOUSE_NAME = '{{ params.warehouse_name }}'
        GROUP BY DATE(START_TIME), WAREHOUSE_NAME
        ORDER BY usage_date DESC
        """,
        params={"warehouse_name": ENV_VARS["SNOWFLAKE_WAREHOUSE"]},
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        doc_md="Analyzes warehouse usage to recommend scaling adjustments",
    )

    # ---------------- Final Steps ----------------
    generate_maintenance_report = SnowflakeOperator(
        task_id="generate_maintenance_report",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Generate maintenance summary
        SELECT
            CURRENT_TIMESTAMP() as maintenance_timestamp,
            'weekly_maintenance_completed' as status,
            (SELECT COUNT(*) FROM SALES_DW.RAW.SALES_RAW) as current_raw_records,
            (SELECT COUNT(*) FROM SALES_DW.MARTS.FACT_SALES) as current_fact_records,
            (SELECT SUM(BYTES)/(1024*1024*1024) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'MARTS') as marts_size_gb
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        doc_md="Generates final maintenance report with key metrics",
    )

    end_maintenance = DummyOperator(
        task_id="end_maintenance",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Completes the weekly maintenance process",
    )

    # ---------------- Task Dependencies ----------------
    start_maintenance >> [
        cleanup_old_raw_data,
        cleanup_s3_old_data,
        cleanup_airflow_logs,
        cleanup_docker_resources,
    ]

    cleanup_old_raw_data >> cleanup_old_staging_data

    [cleanup_old_raw_data, cleanup_old_staging_data] >> calculate_table_stats

    calculate_table_stats >> [optimize_table_clustering, vacuum_tables]

    [optimize_table_clustering, vacuum_tables] >> [
        update_schema_documentation,
        validate_schema_integrity,
        warehouse_scaling_check,
    ]

    [
        cleanup_s3_old_data,
        cleanup_airflow_logs,
        cleanup_docker_resources,
    ] >> kafka_log_cleanup

    (
        [
            update_schema_documentation,
            validate_schema_integrity,
            warehouse_scaling_check,
            kafka_log_cleanup,
        ]
        >> generate_maintenance_report
        >> end_maintenance
    )
