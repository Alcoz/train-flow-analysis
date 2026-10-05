{#- Date of the partition being built (the Dagster partition key), as a SQL DATE. -#}
{% macro run_date() %}
    CAST('{{ var("today", run_started_at.strftime("%Y-%m-%d")) }}' AS DATE)
{%- endmacro %}
