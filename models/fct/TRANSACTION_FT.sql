{{ config(
	    materialized="incremental",
	    schema='FCT',
        insert_exclude_columns="T_ID"
 ) }}

with dt as (select * from {{ref("DATE_DIM")}} ),
merch as (select * from {{ref("MERCHANT_DIM")}} where DBT_VALID_TO IS NULL)
SELECT * FROM (
SELECT 
    TRANSACTION_ID, 
    TRANSACTION_DATE,
    ACCOUNT_ID, 
    M.MERCHANTID AS MERCHANT_ID,
    D.DATEID,
    AMOUNT,
    to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
    to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
    md5(coalesce(cast(TRANSACTION_ID as varchar ), '')
         || '|' || coalesce(cast(ACCOUNT_ID as varchar ), '')
         || '|' || coalesce(cast(MERCHANTID as varchar ), '')
         || '|' || coalesce(cast(DATEID as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
    coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null) as dbt_valid_to
from {{ref("RAW_TRANSACTION")}} T
INNER JOIN
dt D
    ON D.DATE=T.TRANSACTION_DATE
INNER JOIN
merch M
    ON M.MERCHANT_NAME=T.MERCHANT_NAME AND M.MERCHANT_CATEGORY=T.MERCHANT_CATEGORY
) A
{% if is_incremental() %}
where A.TRANSACTION_DATE > (select coalesce(max(TRANSACTION_DATE),'1900-01-01') from {{ this }} )

{% endif %}