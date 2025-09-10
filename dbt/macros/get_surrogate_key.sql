-- macros/get_surrogate_key.sql
-- Generate surrogate keys using MD5 hash of business keys

{% macro get_surrogate_key(field_list) %}
    {{ return(dbt_utils.generate_surrogate_key(field_list)) }}
{% endmacro %}

-- Alternative implementation without dbt_utils dependency
{% macro get_surrogate_key_simple(field_list) %}
    upper(
        md5(
            cast(
                concat(
                    {% for field in field_list %}
                        coalesce(cast({{ field }} as varchar), '')
                        {%- if not loop.last -%},{%- endif -%}
                    {% endfor %}
                ) as varchar
            )
        )
    )
{% endmacro %}

-- Get current timestamp for audit columns
{% macro get_current_timestamp() %}
    {{ return("current_timestamp()") }}
{% endmacro %}
