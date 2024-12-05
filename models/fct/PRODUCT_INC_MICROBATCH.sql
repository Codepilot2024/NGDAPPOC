{{ config(
	    materialized="incremental",
        incremental_strategy='microbatch',
        event_time='TRANSACTION_DATE',
	    schema='FCT',
        begin='2024-04-03',
        batch_size='day'
 ) }}
 
SELECT 
SUP_KEY,
 product_id,
 product_name,
 category,
 price,
 stock_quantity,
 SHA1_HEX(product_id||product_name||category||price||stock_quantity) AS HASH_VAL,
 LOAD_DT,
 to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
 to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as VLD_FROM_TMS,
 md5(coalesce(cast(product_id as varchar ), '')
         || '|' || coalesce(cast(product_name as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
    coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null) as VLD_TO_TMS
FROM
{{ ref('PRODUCT_VW')}} 
