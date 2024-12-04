{{ config(
	    materialized="incremental",
        incremental_strategy='microbatch',
        event_time='TRANSACTION_DATE',
	    schema='FCT',
        begin='2024-02-03',
        batch_size='day'
 ) }}

with dt as (select * from {{ref("DATE_DIM")}} )
SELECT * FROM (
SELECT 
   {{ dbt_utils.generate_surrogate_key(['TRANSACTION_ID','TRANSACTION_DATE','ACCOUNT_ID']) }} as T_ID,
    TRANSACTION_ID, 
    TRANSACTION_DATE,
    ACCOUNT_ID, 
    D.DATEID,
    AMOUNT,
    to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
    to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
    md5(coalesce(cast(TRANSACTION_ID as varchar ), '')
         || '|' || coalesce(cast(ACCOUNT_ID as varchar ), '')
         || '|' || coalesce(cast(DATEID as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
    coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null) as dbt_valid_to
from {{ref("RAW_TRANSACTION")}} T
INNER JOIN
dt D
    ON D.DATE=T.TRANSACTION_DATE
) A
