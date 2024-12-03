{{ config(
	    materialized="view",
	    schema='DIM'
	           ) }}
SELECT
 DISTINCT 
 MERCHANT_NAME,
 MERCHANT_CATEGORY,
 SHA1_HEX(MERCHANT_NAME||MERCHANT_CATEGORY) AS HASH_VAL 
 FROM
 {{ref('RAW_TRANSACTION')}}