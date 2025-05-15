{% macro get_insert_columns(insert_exclude_columns, dest_columns) %}
  {%- set default_cols = dest_columns | map(attribute='name') | list -%}

  {%- if insert_exclude_columns -%}
    {%- set insert_columns = [] -%}
    {%- for column in dest_columns -%}
      {% if column.column | lower not in insert_exclude_columns | map("lower") | list %}
        {%- do insert_columns.append(column.name) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
    {%- set insert_columns = default_cols -%}
  {%- endif -%}

   {%- set quoted_insert_columns = [] -%}
   {% for col in insert_columns %}
        {% do quoted_insert_columns.append((col, model.columns)) %}
   {% endfor %}
   {{ return(quoted_insert_columns)}}
{% endmacro %}