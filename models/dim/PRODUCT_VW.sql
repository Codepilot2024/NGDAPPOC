{{ config(
	    materialized="view",
	    schema='DIM'
	           ) }}
SELECT
 DISTINCT 
 product_id||product_name AS SUP_KEY,
 product_id,
 product_name,
 category,
 price,
 stock_quantity,
 SHA1_HEX(product_id||product_name||category||price||stock_quantity) AS HASH_VAL,
 LOAD_DT 
 FROM
 {{ref('RAW_PRODUCT')}}
