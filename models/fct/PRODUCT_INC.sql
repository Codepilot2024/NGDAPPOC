{{ config(
	    materialized="incremental",
	    schema='FCT'
        ,insert_exclude_columns="T_ID"
 ) }}

 ---
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
{% if is_incremental() %}
where LOAD_DT > (select coalesce(max(LOAD_DT),'1900-01-01') from {{ this }} )
{% endif %}