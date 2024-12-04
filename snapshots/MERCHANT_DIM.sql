{% snapshot MERCHANT_DIM %}
{{
 config(
 target_schema= 'DIM',
 unique_key=['SUP_KEY'],
 strategy='check',
 check_cols=['hash_val'],
 invalidate_hard_deletes=True
 )
}}
select 
{{ dbt_utils.star(from=ref('MERCHANT_VW')) }}
 FROM 
  {{ ref('MERCHANT_VW') }}
{% endsnapshot %}