{% macro parse_datetime(timestamp_string) %}
    CAST('{{ timestamp_string }}' AS datetime)
{% endmacro %}
