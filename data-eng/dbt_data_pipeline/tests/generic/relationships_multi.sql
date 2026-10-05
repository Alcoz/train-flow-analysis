{#-
    Composite-key version of the built-in `relationships` test: every
    non-null combination of `columns` in the model must exist in `to`.
-#}
{% test relationships_multi(model, to, columns, to_columns) %}

SELECT child.*
FROM {{ model }} AS child
LEFT JOIN {{ to }} AS parent
    ON {% for col in columns -%}
        child.{{ col }} = parent.{{ to_columns[loop.index0] }}{% if not loop.last %} AND {% endif %}
    {%- endfor %}
WHERE {% for col in columns -%}
        child.{{ col }} IS NOT NULL AND {% endfor -%}
    parent.{{ to_columns[0] }} IS NULL

{% endtest %}
