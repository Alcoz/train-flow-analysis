{#-
    Reads the partition of the run date of a Gold table written by another
    job. The DuckDB of a run is in memory: a model built by another job only
    exists as files. The model must also declare the dependency with a
    depends_on comment, for dbt and Dagster lineage. The partition column is
    not in the files: select run_date() instead.
-#}
{% macro gold_partition(table_name) %}
    read_parquet('{{ target.external_root }}/{{ table_name }}/date={{ var("today", run_started_at.strftime("%Y-%m-%d")) }}/*.parquet')
{%- endmacro %}
