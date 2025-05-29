{% test vldfrom_gt_vldto(model, column_name, column_2) %}
SELECT *
FROM {{ model }}
WHERE  ({{ column_name }} > {{ column_2 }})
{% endtest %}
