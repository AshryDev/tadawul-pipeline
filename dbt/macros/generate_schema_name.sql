-- Override dbt's default schema name generation.
-- Without this macro, dbt-trino prefixes every schema with the target schema
-- (e.g. "silver_gold"), breaking the bronze/silver/gold separation.
-- This macro uses the custom schema name verbatim when provided.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
