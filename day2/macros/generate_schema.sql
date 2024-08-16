{# スキーマ名の作成 #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {# work環境の場合、WORK_XXXにすべてのmodelを作成する #}
    {%- if target.name in ("work") -%}
        {{ default_schema }}
    {%- else -%} {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}