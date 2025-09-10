-- macros/data_quality.sql
-- Custom macros for data quality checks

-- Test for valid date range
{% macro test_valid_date_range(model, column_name, min_date, max_date) %}
    select *
    from {{ model }}
    where {{ column_name }} < '{{ min_date }}'
       or {{ column_name }} > '{{ max_date }}'
       or {{ column_name }} is null
{% endmacro %}

-- Test for valid business key format
{% macro test_valid_order_id_format(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} is null
       or length({{ column_name }}) < 5
       or {{ column_name }} not regexp '[A-Z]{2}-[0-9]{4,}'
{% endmacro %}

-- Test for reasonable sales amounts
{% macro test_reasonable_sales_amount(model, column_name, min_amount=0, max_amount=1000000) %}
    select *
    from {{ model }}
    where {{ column_name }} < {{ min_amount }}
       or {{ column_name }} > {{ max_amount }}
       or {{ column_name }} is null
{% endmacro %}

-- Test for valid profit margins
{% macro test_valid_profit_margin(model, sales_column, profit_column) %}
    select *
    from {{ model }}
    where {{ sales_column }} > 0
      and (
          {{ profit_column }} / {{ sales_column }} < -1.0  -- Loss more than 100%
          or {{ profit_column }} / {{ sales_column }} > 1.0  -- Profit more than 100%
      )
{% endmacro %}

-- Test for data freshness (custom implementation)
{% macro test_data_freshness(model, timestamp_column, threshold_hours=24) %}
    select *
    from {{ model }}
    where {{ timestamp_column }} < current_timestamp() - interval '{{ threshold_hours }} hours'
    order by {{ timestamp_column }} desc
    limit 1
{% endmacro %}

-- Check for duplicate records based on business key
{% macro test_unique_business_key(model, business_key_columns) %}
    select
        {% for column in business_key_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %},
        count(*) as record_count
    from {{ model }}
    group by
        {% for column in business_key_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    having count(*) > 1
{% endmacro %}
