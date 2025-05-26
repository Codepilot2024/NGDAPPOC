{{ config(
	    materialized="incremental",
        incremental_strategy='microbatch',
        event_time='TRANSACTION_DATE',
	    schema='FCT',
        begin='2024-04-03',
        batch_size='day'
 ) }}
with dt as (select * from {{ref("DATE_DIM")}} )
SELECT 
T.SUP_KEY,
 T.product_id,
 T.product_name,
 T.category,
 T.price,
 T.stock_quantity,
 SHA1_HEX(T.product_id||T.product_name||T.category||T.price||T.stock_quantity) AS HASH_VAL,
 T.LOAD_DT,
 to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
 to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as VLD_FROM_TMS,
 md5(coalesce(cast(T.product_id as varchar ), '')
         || '|' || coalesce(cast(T.product_name as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
    coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null) as VLD_TO_TMS
FROM
{{ ref('PRODUCT_VW')}} T
INNER JOIN
dt D
    ON D.DATE=T.TRANSACTION_DATE
