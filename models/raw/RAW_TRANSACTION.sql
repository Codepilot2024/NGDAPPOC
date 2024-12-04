{{ config(
	    materialized="incremental",
        incremental_strategy="append",
	    schema='STG'
	           ) }}
select
	{{ dbt_utils.star(from=source('CUSTOMER', 'RAW_TRANSACTION')) }},
    CURRENT_TIMESTAMP AS LOAD_DT,
    {{ var('RUN_ID' ) }} AS PM_PCS_ID
from 
{{ source('CUSTOMER', 'RAW_TRANSACTION') }}
{% if is_incremental() %}
where TRANSACTION_DATE > (select coalesce(max(TRANSACTION_DATE),'1900-01-01') from {{ this }} )

{% endif %}