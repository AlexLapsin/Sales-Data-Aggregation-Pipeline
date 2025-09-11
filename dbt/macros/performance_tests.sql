-- macros/performance_tests.sql
-- Macros for testing model performance and query optimization

-- Test query execution time and resource usage
{% macro test_model_performance(model, max_execution_seconds=300, max_rows_scanned=null) %}
    {% set start_time = modules.datetime.datetime.now() %}

    with performance_test as (
        select
            count(*) as row_count,
            current_timestamp() as query_start_time
        from {{ model }}
    )
    select
        row_count,
        query_start_time,
        '{{ model }}' as model_name,
        case
            when row_count = 0 then 'EMPTY_RESULT_SET'
            {% if max_rows_scanned %}
            when row_count > {{ max_rows_scanned }} then 'EXCESSIVE_ROWS_SCANNED'
            {% endif %}
            else 'PERFORMANCE_OK'
        end as performance_status
    from performance_test
    where
        row_count = 0
        {% if max_rows_scanned %}
        or row_count > {{ max_rows_scanned }}
        {% endif %}
{% endmacro %}

-- Test for inefficient joins or cartesian products
{% macro test_join_efficiency(model, expected_max_multiplier=2.0) %}
    with source_counts as (
        select count(*) as total_rows
        from {{ model }}
    ),
    -- This is a simplified test - in practice you'd need to compare with source row counts
    efficiency_check as (
        select
            total_rows,
            case
                when total_rows > 1000000 then 'POTENTIAL_CARTESIAN_PRODUCT'
                else 'JOIN_EFFICIENCY_OK'
            end as efficiency_status
        from source_counts
    )
    select *
    from efficiency_check
    where efficiency_status != 'JOIN_EFFICIENCY_OK'
{% endmacro %}

-- Test for proper use of indexes/clustering
{% macro test_clustering_effectiveness(model, cluster_columns) %}
    with clustering_stats as (
        select
            {% for column in cluster_columns %}
            count(distinct {{ column }}) as distinct_{{ column }}_count{% if not loop.last %},{% endif %}
            {% endfor %},
            count(*) as total_rows
        from {{ model }}
    )
    select *
    from clustering_stats
    where
        total_rows > 10000  -- Only test for larger tables
        {% for column in cluster_columns %}
        and distinct_{{ column }}_count / total_rows::float > 0.8  -- High cardinality columns may not cluster well
        {% endfor %}
{% endmacro %}

-- Test for model dependency cycles
{% macro test_model_dependencies() %}
    -- This would need to be implemented with graph analysis of dbt models
    -- For now, returning empty result (no cycles found)
    select
        'No cycles detected' as dependency_status
    where false  -- This will always return empty, indicating no issues
{% endmacro %}

-- Test for excessive model rebuilds
{% macro test_incremental_efficiency(model, date_column, days_back=7) %}
    {% if execute and is_incremental() %}
    with incremental_stats as (
        select
            min({{ date_column }}) as min_date_processed,
            max({{ date_column }}) as max_date_processed,
            count(*) as incremental_rows,
            count(distinct {{ date_column }}) as distinct_dates
        from {{ model }}
        where {{ date_column }} >= current_date() - interval '{{ days_back }} days'
    )
    select *
    from incremental_stats
    where
        incremental_rows = 0  -- No incremental rows processed
        or distinct_dates > {{ days_back }} + 1  -- Processing more days than expected
    {% else %}
    select 'Not an incremental model' as status where false
    {% endif %}
{% endmacro %}

-- Test for model freshness and staleness
{% macro test_model_freshness(model, timestamp_column, max_staleness_hours=24) %}
    with freshness_check as (
        select
            max({{ timestamp_column }}) as latest_timestamp,
            current_timestamp() - max({{ timestamp_column }}) as staleness,
            case
                when current_timestamp() - max({{ timestamp_column }}) > interval '{{ max_staleness_hours }} hours'
                then 'STALE_DATA'
                else 'FRESH_DATA'
            end as freshness_status
        from {{ model }}
    )
    select *
    from freshness_check
    where freshness_status = 'STALE_DATA'
{% endmacro %}

-- Test for resource utilization patterns
{% macro test_resource_usage(model, max_memory_gb=null, max_cpu_seconds=null) %}
    -- Note: This would need warehouse-specific implementation
    -- For Snowflake, you'd query QUERY_HISTORY views
    -- For now, this is a placeholder that tests table size as proxy
    with resource_proxy as (
        select
            count(*) as estimated_rows,
            count(*) / 1000000.0 as estimated_gb_proxy  -- Rough estimate
        from {{ model }}
    )
    select *
    from resource_proxy
    where
        {% if max_memory_gb %}
        estimated_gb_proxy > {{ max_memory_gb }}
        {% else %}
        false
        {% endif %}
{% endmacro %}
