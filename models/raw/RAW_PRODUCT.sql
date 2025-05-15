{{ config(
	    materialized="table",
	    schema='STG'
	           ) }}
select

	{{ dbt_utils.star(from=source('CUSTOMER', 'RAW_PRODUCT')) }}, 
    PRODUCT_ID||PRODUCT_NAME AS SUP_KEY,
    CURRENT_TIMESTAMP AS LOAD_DT,
    {{ var('RUN_ID') }} AS PM_PCS_ID,
    '{{env_var('DBT_USER')}}' as user_id
from 
{{ source('CUSTOMER', 'RAW_PRODUCT') }}