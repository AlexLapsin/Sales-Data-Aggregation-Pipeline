-- macros/audit_columns.sql
-- Standardized audit columns for all models

-- Add standard audit columns to any model
{% macro add_audit_columns() %}
    {{ get_current_timestamp() }} as dbt_created_at,
    {{ get_current_timestamp() }} as dbt_updated_at,
    '{{ invocation_id }}' as dbt_invocation_id,
    '{{ target.name }}' as dbt_target_name
{% endmacro %}

-- Add audit columns for SCD2 tables
{% macro add_scd2_columns(effective_from_field='effective_from', effective_to_field='effective_to') %}
    {{ effective_from_field }},
    {{ effective_to_field }},
    case
        when {{ effective_to_field }} = '2099-12-31' then true
        else false
    end as is_current,
    {{ add_audit_columns() }}
{% endmacro %}

-- Get source system identifier
{% macro get_source_system(source_name) %}
    case
        when '{{ source_name }}' = 'sales_raw' then 'KAFKA_STREAMING'
        when '{{ source_name }}' = 'sales_batch_raw' then 'SPARK_BATCH'
        when '{{ source_name }}' = 'product_raw' then 'PRODUCT_MASTER'
        when '{{ source_name }}' = 'store_raw' then 'STORE_MASTER'
        else 'UNKNOWN'
    end as source_system
{% endmacro %}

-- Standardize column naming (uppercase for Snowflake)
{% macro standardize_column_name(column_name) %}
    {{ return(column_name | upper) }}
{% endmacro %}
