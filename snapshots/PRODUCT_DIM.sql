{% snapshot PRODUCT_DIM %}
{{
 config(
 target_schema= 'DIM',
 unique_key=['SUP_KEY'],
 strategy='check',
 check_cols=['hash_val'],
 invalidate_hard_deletes='True'
 )
}}
select 
{{ dbt_utils.star(from=ref('PRODUCT_VW')) }}
 FROM 
  {{ ref('PRODUCT_VW') }}
{% endsnapshot %}