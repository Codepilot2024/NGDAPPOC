{% snapshot FULL_CUD_V_SNAPSHOT %}
{{
 config(
 target_schema= 'IS_PMS_CDS_OWNER',
 unique_key=['IP_SUP_KEY','VLD_FROM_TMS','VLD_TO_TMS'],
 strategy='check',
 check_cols=['HASH_VAL'],
 valid_from='vld_from_tms',
 valid_to='vld_to_tms',
 updated_at='UDT_TMS',
 invalidate_hard_deletes=false
 )
}}
select 
{{ dbt_utils.star(from=ref('FULL_CUD_V_VW')) }}
 FROM 
  {{ ref('FULL_CUD_V_VW') }}
{% endsnapshot %}