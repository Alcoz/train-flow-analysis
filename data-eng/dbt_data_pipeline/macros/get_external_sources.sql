{% macro duckdb__get_external_source(source_node) %}
    read_parquet('{{ source_node.meta.external_location }}')
{% endmacro %}