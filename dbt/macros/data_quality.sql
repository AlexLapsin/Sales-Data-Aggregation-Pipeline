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

-- Test for acceptable null rates in critical columns
{% macro test_null_rate(model, column_name, max_null_rate=0.05) %}
    with null_stats as (
        select
            count(*) as total_records,
            sum(case when {{ column_name }} is null then 1 else 0 end) as null_records,
            (sum(case when {{ column_name }} is null then 1 else 0 end) / count(*)::float) as null_rate
        from {{ model }}
    )
    select *
    from null_stats
    where null_rate > {{ max_null_rate }}
{% endmacro %}

-- Test for data distribution anomalies
{% macro test_data_distribution(model, column_name, expected_distinct_count_min=null, expected_distinct_count_max=null) %}
    with distribution_stats as (
        select
            count(*) as total_records,
            count(distinct {{ column_name }}) as distinct_values,
            count(distinct {{ column_name }}) / count(*)::float as uniqueness_ratio
        from {{ model }}
    )
    select *
    from distribution_stats
    where
        ({{ expected_distinct_count_min }} is not null and distinct_values < {{ expected_distinct_count_min }})
        or ({{ expected_distinct_count_max }} is not null and distinct_values > {{ expected_distinct_count_max }})
{% endmacro %}

-- Test for referential integrity between models
{% macro test_referential_integrity(child_model, parent_model, child_key, parent_key) %}
    select {{ child_key }}
    from {{ child_model }}
    where {{ child_key }} is not null
      and {{ child_key }} not in (
          select {{ parent_key }}
          from {{ parent_model }}
          where {{ parent_key }} is not null
      )
{% endmacro %}

-- Test for row count stability (comparing to previous runs)
{% macro test_row_count_stability(model, min_expected_rows=null, max_expected_rows=null, variance_threshold=0.20) %}
    with current_count as (
        select count(*) as current_row_count
        from {{ model }}
    ),
    validation as (
        select
            current_row_count,
            case
                when {{ min_expected_rows }} is not null and current_row_count < {{ min_expected_rows }} then false
                when {{ max_expected_rows }} is not null and current_row_count > {{ max_expected_rows }} then false
                else true
            end as within_bounds
        from current_count
    )
    select *
    from validation
    where not within_bounds
{% endmacro %}

-- Test for sequential date/timestamp integrity
{% macro check_sequential_dates(model, date_column, partition_column=null) %}
    with date_gaps as (
        select
            {{ date_column }} as current_date,
            lag({{ date_column }}) over (
                {% if partition_column %}partition by {{ partition_column }} {% endif %}
                order by {{ date_column }}
            ) as previous_date,
            {{ date_column }} - lag({{ date_column }}) over (
                {% if partition_column %}partition by {{ partition_column }} {% endif %}
                order by {{ date_column }}
            ) as date_diff
        from {{ model }}
        where {{ date_column }} is not null
    )
    select *
    from date_gaps
    where date_diff > interval '1 day'  -- Gaps larger than 1 day
       or date_diff < interval '0 days'  -- Dates out of order
{% endmacro %}

-- Test for business rule compliance across related columns
{% macro test_business_rule_compliance(model, rule_expression, rule_description) %}
    select
        *,
        '{{ rule_description }}' as failed_rule
    from {{ model }}
    where not ({{ rule_expression }})
{% endmacro %}

-- Test for data consistency across related models
{% macro test_cross_model_consistency(model1, model2, join_condition, comparison_columns) %}
    with model1_data as (
        select * from {{ model1 }}
    ),
    model2_data as (
        select * from {{ model2 }}
    ),
    comparison as (
        select
            m1.*,
            {% for col in comparison_columns %}
            m2.{{ col }} as model2_{{ col }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from model1_data m1
        inner join model2_data m2 on {{ join_condition }}
        where
            {% for col in comparison_columns %}
            m1.{{ col }} != m2.{{ col }}{% if not loop.last %} or {% endif %}
            {% endfor %}
    )
    select * from comparison
{% endmacro %}
