{#-
    Reads every partition of a Gold table written by another job, with its
    `date` partition column (hive partitioning). Filter on date in the model:
    DuckDB then only reads the files of the days kept, and a day with no file
    returns no rows instead of failing. The model must also declare the
    dependency with a depends_on comment, as with gold_partition.
-#}
{% macro gold_history(table_name) %}
    read_parquet('{{ target.external_root }}/{{ table_name }}/date=*/*.parquet', hive_partitioning = true)
{%- endmacro %}
