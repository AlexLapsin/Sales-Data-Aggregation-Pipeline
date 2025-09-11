-- macros/generic_tests.sql
-- Custom generic tests that can be applied to any model/column

-- Test for acceptable value ranges with custom error messages
{% test acceptable_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}

    with validation as (
        select
            {{ column_name }} as test_column,
            case
                {% if min_value is not none and max_value is not none %}
                    {% if inclusive %}
                    when {{ column_name }} >= {{ min_value }} and {{ column_name }} <= {{ max_value }} then true
                    {% else %}
                    when {{ column_name }} > {{ min_value }} and {{ column_name }} < {{ max_value }} then true
                    {% endif %}
                {% elif min_value is not none %}
                    {% if inclusive %}
                    when {{ column_name }} >= {{ min_value }} then true
                    {% else %}
                    when {{ column_name }} > {{ min_value }} then true
                    {% endif %}
                {% elif max_value is not none %}
                    {% if inclusive %}
                    when {{ column_name }} <= {{ max_value }} then true
                    {% else %}
                    when {{ column_name }} < {{ max_value }} then true
                    {% endif %}
                {% endif %}
                else false
            end as is_valid
        from {{ model }}
        where {{ column_name }} is not null
    )

    select test_column
    from validation
    where not is_valid

{% endtest %}

-- Test for string length constraints
{% test string_length(model, column_name, min_length=none, max_length=none) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and (
          {% if min_length is not none %}
          length({{ column_name }}) < {{ min_length }}
          {% endif %}
          {% if min_length is not none and max_length is not none %}
          or
          {% endif %}
          {% if max_length is not none %}
          length({{ column_name }}) > {{ max_length }}
          {% endif %}
      )

{% endtest %}

-- Test for proper email format
{% test valid_email_format(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and not regexp_like({{ column_name }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')

{% endtest %}

-- Test for valid date sequences (no gaps in date series)
{% test sequential_dates(model, date_column, partition_by=none, max_gap_days=1) %}

    with date_analysis as (
        select
            {{ date_column }},
            {% if partition_by %}
            {{ partition_by }},
            {% endif %}
            lag({{ date_column }}) over (
                {% if partition_by %}partition by {{ partition_by }}{% endif %}
                order by {{ date_column }}
            ) as prev_date
        from {{ model }}
        where {{ date_column }} is not null
    ),

    gaps as (
        select
            {{ date_column }},
            {% if partition_by %}
            {{ partition_by }},
            {% endif %}
            prev_date,
            {{ date_column }} - prev_date as gap_days
        from date_analysis
        where prev_date is not null
          and {{ date_column }} - prev_date > {{ max_gap_days }}
    )

    select * from gaps

{% endtest %}

-- Test for reasonable cardinality (not too few, not too many distinct values)
{% test reasonable_cardinality(model, column_name, min_distinct_count=none, max_distinct_count=none, min_distinct_percent=none, max_distinct_percent=none) %}

    with cardinality_stats as (
        select
            count(*) as total_rows,
            count(distinct {{ column_name }}) as distinct_count,
            (count(distinct {{ column_name }}) * 100.0 / count(*)) as distinct_percent
        from {{ model }}
    )

    select *
    from cardinality_stats
    where
        {% if min_distinct_count is not none %}
        distinct_count < {{ min_distinct_count }}
        {% endif %}
        {% if max_distinct_count is not none %}
        {% if min_distinct_count is not none %}or{% endif %}
        distinct_count > {{ max_distinct_count }}
        {% endif %}
        {% if min_distinct_percent is not none %}
        {% if min_distinct_count is not none or max_distinct_count is not none %}or{% endif %}
        distinct_percent < {{ min_distinct_percent }}
        {% endif %}
        {% if max_distinct_percent is not none %}
        {% if min_distinct_count is not none or max_distinct_count is not none or min_distinct_percent is not none %}or{% endif %}
        distinct_percent > {{ max_distinct_percent }}
        {% endif %}

{% endtest %}

-- Test for mutually exclusive boolean columns
{% test mutually_exclusive_ranges(model, lower_bound_column, upper_bound_column, include_nulls=false) %}

    select *
    from {{ model }}
    where
        {% if not include_nulls %}
        {{ lower_bound_column }} is not null
        and {{ upper_bound_column }} is not null
        and
        {% endif %}
        {{ lower_bound_column }} > {{ upper_bound_column }}

{% endtest %}

-- Test for consistent aggregations across related columns
{% test consistent_aggregation(model, detail_column, aggregate_column, aggregation_type='sum', tolerance=0.01) %}

    with aggregation_check as (
        select
            {% if aggregation_type == 'sum' %}
            sum({{ detail_column }}) as calculated_aggregate,
            {% elif aggregation_type == 'avg' %}
            avg({{ detail_column }}) as calculated_aggregate,
            {% elif aggregation_type == 'count' %}
            count({{ detail_column }}) as calculated_aggregate,
            {% elif aggregation_type == 'min' %}
            min({{ detail_column }}) as calculated_aggregate,
            {% elif aggregation_type == 'max' %}
            max({{ detail_column }}) as calculated_aggregate,
            {% endif %}
            max({{ aggregate_column }}) as stored_aggregate  -- Assuming single value per group
        from {{ model }}
    )

    select *
    from aggregation_check
    where abs(calculated_aggregate - stored_aggregate) > {{ tolerance }}

{% endtest %}

-- Test for valid foreign key relationships with detailed error info
{% test enhanced_relationships(model, column_name, to, field, ignore_null=true) %}

    with missing_relationships as (
        select
            {{ column_name }} as orphaned_key,
            count(*) as occurrence_count
        from {{ model }}
        where
            {% if ignore_null %}
            {{ column_name }} is not null
            and
            {% endif %}
            {{ column_name }} not in (
                select {{ field }}
                from {{ to }}
                where {{ field }} is not null
            )
        group by {{ column_name }}
    )

    select * from missing_relationships

{% endtest %}

-- Test for data freshness with business day considerations
{% test business_day_freshness(model, timestamp_column, max_age_business_days=1, timezone='UTC') %}

    with freshness_check as (
        select
            max({{ timestamp_column }}) as latest_timestamp,
            current_timestamp() as current_time,
            -- Simple business day calculation (excludes weekends)
            case
                when extract(dayofweek from current_date()) in (1, 7) then  -- Sunday or Saturday
                    current_date() - interval '{{ max_age_business_days + 2 }} days'
                else
                    current_date() - interval '{{ max_age_business_days }} days'
            end as cutoff_date
        from {{ model }}
    )

    select *
    from freshness_check
    where latest_timestamp < cutoff_date

{% endtest %}

-- Test for proper SCD2 implementation
{% test scd2_integrity(model, business_key, effective_date_column, expiration_date_column, is_current_column) %}

    with scd2_issues as (
        -- Check for overlapping effective periods
        select
            'OVERLAPPING_PERIODS' as issue_type,
            {{ business_key }} as business_key_value,
            count(*) as issue_count
        from {{ model }} a
        inner join {{ model }} b
            on a.{{ business_key }} = b.{{ business_key }}
            and a.{{ effective_date_column }} != b.{{ effective_date_column }}
            and a.{{ effective_date_column }} <= b.{{ expiration_date_column }}
            and b.{{ effective_date_column }} <= a.{{ expiration_date_column }}
        group by {{ business_key }}

        union all

        -- Check for multiple current records
        select
            'MULTIPLE_CURRENT' as issue_type,
            {{ business_key }} as business_key_value,
            count(*) as issue_count
        from {{ model }}
        where {{ is_current_column }} = true
        group by {{ business_key }}
        having count(*) > 1

        union all

        -- Check for invalid date ranges
        select
            'INVALID_DATE_RANGE' as issue_type,
            {{ business_key }} as business_key_value,
            count(*) as issue_count
        from {{ model }}
        where {{ effective_date_column }} > {{ expiration_date_column }}
        group by {{ business_key }}
    )

    select * from scd2_issues

{% endtest %}
