{#- GTFS version (publication day) of a row read with filename = true, from its date=YYYY-MM-DD folder. -#}
{% macro gtfs_version(filename_column='filename') %}
    regexp_extract({{ filename_column }}, 'date=([0-9-]+)/', 1)::DATE
{%- endmacro %}
