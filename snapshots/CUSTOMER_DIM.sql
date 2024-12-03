{% snapshot CUSTOMER_DIM %}
{{
 config(
 target_schema= 'DIM',
 unique_key=['CUST_ID','CUSTOMER_NAME'],
 strategy='check',
 check_cols=['hash_val'],
 invalidate_hard_deletes=True
 )
}}
select 
{{ dbt_utils.star(from=ref('CUSTOMER_VW')) }}
 FROM 
  {{ ref('CUSTOMER_VW') }}
{% endsnapshot %}