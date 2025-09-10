-- macros/generate_schema_name.sql
-- Custom schema naming logic for multi-environment support

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        {# In production, use the custom schema name directly #}
        {{ custom_schema_name | trim }}

    {%- else -%}
        {# In dev/staging, prefix with default schema #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
