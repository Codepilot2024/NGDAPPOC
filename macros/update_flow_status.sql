{% macro update_flow_status(appl_cd, flow_cd, lookup_query) %}
    {% set results = run_query(lookup_query) %}
    
    {% if results and results.columns[0].values() | length > 0 %}
        {% set last_processing_tms = results.columns[0].values()[0] %}
        {% set next_processing_tms = results.columns[1].values()[0] %}

        {% set update_sql %}
            UPDATE CCL_CAS_OWNER.FLOW_STATUS
            SET 
                last_processing_tms = '{{ next_processing_tms }}',
                next_processing_tms = DATETIME_ADD(DATETIME('{{ next_processing_tms }}'),INTERVAL 1 MONTH) 
           
            WHERE 
                application_cd = '{{ appl_cd }}'
                AND flow_cd = '{{ flow_cd }}'
        {% endset %}

        {{ log("Executing update: " ~ update_sql, info=True) }}
        {{ run_query(update_sql) }}
    {% else %}
        {{ log("No results returned from lookup query.", warning=True) }}
    {% endif %}
{% endmacro %}
