{{ config(
	    materialized="table",
	    schema='STG'
	           ) }}
select
	{{ dbt_utils.star(from=source('CUSTOMER', 'RAW_TRANSACTION')) }},
    CURRENT_TIMESTAMP AS LOAD_DT,
    {{ var('RUN_ID' ) }} AS PM_PCS_ID
from 
{{ source('CUSTOMER', 'RAW_TRANSACTION') }}