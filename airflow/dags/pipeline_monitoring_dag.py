# airflow/dags/pipeline_monitoring_dag.py
"""
Pipeline Monitoring and Alerting DAG
Monitors the health and performance of the cloud sales pipeline:
1. Data freshness monitoring
2. Pipeline performance metrics
3. Data quality trend analysis
4. Infrastructure health checks
5. Automated alerting and reporting
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
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
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# Environment variables
ENV_VARS = {
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}


# ---------------- Helper Functions ----------------
def generate_daily_report(**context):
    """Generate comprehensive daily pipeline report"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default",
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="MARTS",
    )

    # Get daily metrics
    daily_metrics_query = """
    WITH daily_stats AS (
        SELECT
            DATE_KEY,
            SUM(TRANSACTION_COUNT) as total_transactions,
            SUM(TOTAL_NET_SALES) as total_sales,
            SUM(UNIQUE_CUSTOMERS) as unique_customers,
            AVG(AVG_DATA_QUALITY_SCORE) as avg_quality_score
        FROM FACT_SALES_DAILY
        WHERE DATE_KEY = TO_NUMBER(TO_CHAR(CURRENT_DATE() - INTERVAL '1 day', 'YYYYMMDD'))
        GROUP BY DATE_KEY
    ),
    prev_day_stats AS (
        SELECT
            SUM(TRANSACTION_COUNT) as prev_transactions,
            SUM(TOTAL_NET_SALES) as prev_sales,
            SUM(UNIQUE_CUSTOMERS) as prev_customers
        FROM FACT_SALES_DAILY
        WHERE DATE_KEY = TO_NUMBER(TO_CHAR(CURRENT_DATE() - INTERVAL '2 days', 'YYYYMMDD'))
    )
    SELECT
        d.*,
        p.prev_transactions,
        p.prev_sales,
        p.prev_customers,
        ROUND(((d.total_transactions - p.prev_transactions) / NULLIF(p.prev_transactions, 0) * 100), 2) as transaction_growth,
        ROUND(((d.total_sales - p.prev_sales) / NULLIF(p.prev_sales, 0) * 100), 2) as sales_growth
    FROM daily_stats d
    CROSS JOIN prev_day_stats p
    """

    try:
        result = hook.get_first(daily_metrics_query)
        if result:
            report_data = {
                "date": context["ds"],
                "total_transactions": result[1] or 0,
                "total_sales": float(result[2] or 0),
                "unique_customers": result[3] or 0,
                "avg_quality_score": float(result[4] or 0),
                "transaction_growth": float(result[7] or 0),
                "sales_growth": float(result[8] or 0),
            }

            context["task_instance"].xcom_push(key="daily_report", value=report_data)
            return report_data
        else:
            # No data for yesterday
            context["task_instance"].xcom_push(
                key="daily_report", value={"error": "No data available"}
            )
            return {"error": "No data available for yesterday"}

    except Exception as e:
        error_msg = f"Failed to generate daily report: {str(e)}"
        context["task_instance"].xcom_push(
            key="daily_report", value={"error": error_msg}
        )
        raise


def check_data_quality_trends(**context):
    """Analyze data quality trends over the past week"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default",
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="MARTS",
    )

    quality_trend_query = """
    SELECT
        d.DATE_VALUE,
        AVG(f.DATA_QUALITY_SCORE) as avg_daily_quality,
        COUNT(f.SALES_KEY) as record_count,
        COUNT(CASE WHEN f.DATA_QUALITY_SCORE < 80 THEN 1 END) as low_quality_count,
        ROUND(COUNT(CASE WHEN f.DATA_QUALITY_SCORE < 80 THEN 1 END) / COUNT(f.SALES_KEY) * 100, 2) as low_quality_pct
    FROM FACT_SALES f
    INNER JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
    WHERE d.DATE_VALUE >= CURRENT_DATE() - INTERVAL '7 days'
    GROUP BY d.DATE_VALUE
    ORDER BY d.DATE_VALUE
    """

    try:
        results = hook.get_records(quality_trend_query)

        quality_issues = []
        for row in results:
            (
                date_val,
                avg_quality,
                record_count,
                low_quality_count,
                low_quality_pct,
            ) = row

            # Flag quality issues
            if avg_quality < 85:
                quality_issues.append(
                    {
                        "date": str(date_val),
                        "avg_quality": float(avg_quality),
                        "issue": "Low average quality score",
                    }
                )

            if low_quality_pct > 10:  # More than 10% low quality records
                quality_issues.append(
                    {
                        "date": str(date_val),
                        "low_quality_pct": float(low_quality_pct),
                        "issue": "High percentage of low quality records",
                    }
                )

        context["task_instance"].xcom_push(key="quality_issues", value=quality_issues)
        return quality_issues

    except Exception as e:
        error_msg = f"Failed to check quality trends: {str(e)}"
        context["task_instance"].xcom_push(
            key="quality_issues", value=[{"error": error_msg}]
        )
        raise


# ---------------- DAG Definition ----------------
with DAG(
    dag_id="pipeline_monitoring",
    description="Monitors pipeline health, performance, and data quality",
    default_args=default_args,
    schedule_interval="0 8 * * *",  # Run daily at 8 AM (after main pipeline)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "data-quality", "alerts"],
    max_active_runs=1,
) as dag:

    start_monitoring = DummyOperator(
        task_id="start_monitoring",
        doc_md="Starts the daily pipeline monitoring and reporting process",
    )

    # ---------------- Data Freshness Checks ----------------
    check_streaming_freshness = SnowflakeOperator(
        task_id="check_streaming_freshness",
        snowflake_conn_id="snowflake_default",
        sql="""
        SELECT
            'streaming_data_freshness' as check_name,
            MAX(INGESTION_TIMESTAMP) as latest_timestamp,
            COUNT(*) as record_count,
            DATEDIFF('hour', MAX(INGESTION_TIMESTAMP), CURRENT_TIMESTAMP()) as hours_since_last_record
        FROM SALES_DW.RAW.SALES_RAW
        WHERE PARTITION_DATE >= CURRENT_DATE() - INTERVAL '1 day'
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="RAW",
        doc_md="Checks freshness of streaming data from Kafka",
    )

    check_batch_freshness = SnowflakeOperator(
        task_id="check_batch_freshness",
        snowflake_conn_id="snowflake_default",
        sql="""
        SELECT
            'batch_data_freshness' as check_name,
            MAX(INGESTION_TIMESTAMP) as latest_timestamp,
            COUNT(*) as record_count,
            COUNT(DISTINCT BATCH_ID) as unique_batches
        FROM SALES_DW.RAW.SALES_BATCH_RAW
        WHERE PARTITION_DATE >= CURRENT_DATE() - INTERVAL '1 day'
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="RAW",
        doc_md="Checks freshness of batch data from Spark ETL",
    )

    # ---------------- Performance Monitoring ----------------
    pipeline_performance_check = SnowflakeOperator(
        task_id="pipeline_performance_check",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Check pipeline execution times from Snowflake query history
        SELECT
            'pipeline_performance' as check_name,
            DATE(START_TIME) as execution_date,
            AVG(EXECUTION_TIME / 1000) as avg_execution_seconds,
            MAX(EXECUTION_TIME / 1000) as max_execution_seconds,
            COUNT(*) as query_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= CURRENT_DATE() - INTERVAL '7 days'
            AND QUERY_TEXT LIKE '%dbt%'
            AND EXECUTION_STATUS = 'SUCCESS'
        GROUP BY DATE(START_TIME)
        ORDER BY execution_date DESC
        LIMIT 7
        """,
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        database=ENV_VARS["SNOWFLAKE_DATABASE"],
        schema="MARTS",
        doc_md="Monitors pipeline execution performance trends",
    )

    # ---------------- Data Quality Monitoring ----------------
    generate_daily_report_task = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_daily_report,
        doc_md="Generates comprehensive daily pipeline metrics report",
    )

    check_quality_trends_task = PythonOperator(
        task_id="check_quality_trends",
        python_callable=check_data_quality_trends,
        doc_md="Analyzes data quality trends and identifies issues",
    )

    # ---------------- Infrastructure Health ----------------
    kafka_cluster_health = DockerOperator(
        task_id="kafka_cluster_health",
        image="confluentinc/cp-kafka:latest",
        command=[
            "bash",
            "-c",
            f"""
            kafka-topics --bootstrap-server {ENV_VARS['KAFKA_BOOTSTRAP_SERVERS']} --list > /tmp/topics.txt &&
            echo "Available topics:" && cat /tmp/topics.txt &&
            kafka-consumer-groups --bootstrap-server {ENV_VARS['KAFKA_BOOTSTRAP_SERVERS']} --list > /tmp/groups.txt &&
            echo "Consumer groups:" && cat /tmp/groups.txt
            """,
        ],
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="host",
        doc_md="Checks Kafka cluster health and lists topics/consumer groups",
    )

    snowflake_warehouse_health = SnowflakeOperator(
        task_id="snowflake_warehouse_health",
        snowflake_conn_id="snowflake_default",
        sql="""
        SELECT
            'warehouse_health' as check_name,
            WAREHOUSE_NAME,
            STATE,
            SIZE,
            RUNNING,
            QUEUED,
            IS_DEFAULT,
            IS_CURRENT,
            AUTO_SUSPEND,
            AUTO_RESUME
        FROM INFORMATION_SCHEMA.WAREHOUSES
        WHERE WAREHOUSE_NAME = '{{ params.warehouse_name }}'
        """,
        params={"warehouse_name": ENV_VARS["SNOWFLAKE_WAREHOUSE"]},
        warehouse=ENV_VARS["SNOWFLAKE_WAREHOUSE"],
        doc_md="Checks Snowflake warehouse status and configuration",
    )

    # ---------------- Alerting and Reporting ----------------
    daily_report_email = EmailOperator(
        task_id="daily_report_email",
        to=[os.getenv("ALERT_EMAIL", "data-team@company.com")],
        subject="Daily Sales Pipeline Report - {{ ds }}",
        html_content="""
        <h2>Daily Sales Pipeline Report</h2>
        <p><strong>Execution Date:</strong> {{ ds }}</p>

        <h3>Daily Metrics</h3>
        <ul>
            <li><strong>Total Transactions:</strong> {{ ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')['total_transactions'] | default('N/A') }}</li>
            <li><strong>Total Sales:</strong> ${{ ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')['total_sales'] | default('N/A') | round(2) }}</li>
            <li><strong>Unique Customers:</strong> {{ ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')['unique_customers'] | default('N/A') }}</li>
            <li><strong>Average Quality Score:</strong> {{ ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')['avg_quality_score'] | default('N/A') | round(2) }}</li>
        </ul>

        <h3>📈 Growth Metrics</h3>
        <ul>
            <li><strong>Transaction Growth:</strong> {{ ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')['transaction_growth'] | default('N/A') }}%</li>
            <li><strong>Sales Growth:</strong> {{ ti.xcom_pull(key='daily_report', task_ids='generate_daily_report')['sales_growth'] | default('N/A') }}%</li>
        </ul>

        <h3>Quality Issues</h3>
        {% set quality_issues = ti.xcom_pull(key='quality_issues', task_ids='check_quality_trends') %}
        {% if quality_issues %}
            {% for issue in quality_issues %}
            <p style="color: orange;">⚠ {{ issue.date }}: {{ issue.issue }}</p>
            {% endfor %}
        {% else %}
            <p style="color: green;">No quality issues detected</p>
        {% endif %}

        <p><em>This is an automated report from the Sales Data Pipeline monitoring system.</em></p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Sends daily pipeline report via email",
    )

    quality_alert_slack = SlackWebhookOperator(
        task_id="quality_alert_slack",
        http_conn_id="slack_webhook",
        message="""
        Data Quality Alert - {{ ds }}

        {% set quality_issues = ti.xcom_pull(key='quality_issues', task_ids='check_quality_trends') %}
        {% if quality_issues and quality_issues|length > 0 %}
        Quality issues detected:
        {% for issue in quality_issues %}
        • {{ issue.date }}: {{ issue.issue }}
        {% endfor %}

        Please investigate and take corrective action.
        {% endif %}
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Sends Slack alerts for data quality issues",
    )

    end_monitoring = DummyOperator(
        task_id="end_monitoring",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Completes the pipeline monitoring process",
    )

    # ---------------- Task Dependencies ----------------
    start_monitoring >> [
        check_streaming_freshness,
        check_batch_freshness,
        pipeline_performance_check,
        kafka_cluster_health,
        snowflake_warehouse_health,
    ]

    (
        [check_streaming_freshness, check_batch_freshness, pipeline_performance_check]
        >> generate_daily_report_task
        >> check_quality_trends_task
    )

    (
        [
            generate_daily_report_task,
            check_quality_trends_task,
            kafka_cluster_health,
            snowflake_warehouse_health,
        ]
        >> daily_report_email
        >> quality_alert_slack
        >> end_monitoring
    )
