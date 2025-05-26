{{ config(
    schema=var('PS_DB_IS_ORA_SCM_v0001.$ORA_SCM_SAC') ,
    materialized='incremental',
    pre_hook='CALL {{ var("PS_DB_IS_ORA_SCM_v0001.$ORA_SCM_SAC") }}.proc_truncate_table(\'{{ var("PS_DB_IS_ORA_SCM_v0001.$ORA_SCM_SAC") }}\',\'PMS_IS_SAC_DPL_SS_DPL_IP_TCV_FULL_CUD_V\');'
) }}

   --- 

SELECT
            'DPL'|| '|' || 'IP_TCV_FULL_CUD' || '|' || tc_obj_instance_key_3|| '&' 
            || tc_obj_instance_key_2 || '&' || tc_obj_instance_key_1                                            AS ip_sup_key,
            'DPL'                                                                                               AS mstr_src_stm_cd,
            'IP_TCV_FULL_CUD'                                                                                   AS ent_tp_cd,
             tc_obj_instance_key_3|| '&' || tc_obj_instance_key_2 || '&' || tc_obj_instance_key_1               AS mstr_src_stm_key,
            'DPL'                                                                                               AS src_stm_cd,
            'N'                                                                                                 AS del_in_src_stm_ind,
            {{ parse_datetime(var("pm_pcs_period")) }}                                                          AS vld_from_tms,
            {{ var("pm_pcs_id") }}                                                                              AS pcs_isrt_id,
            current_datetime                                                                                    AS isrt_tms,
            src_instn_id                                                                                        AS src_instn_isrt_id,
            tc_obj_instance_key_1                                                                               AS tc_obj_instance_key_1,
            tc_obj_instance_key_2                                                                               AS tc_obj_instance_key_2,
            tc_obj_instance_key_3                                                                               AS tc_obj_instance_key_3,
            tc_data_type_char_ind                                                                               AS tc_data_type_char_ind_changed_name,
            tc_data_type_nchar_cd                                                                               AS tc_data_type_nchar_cd,
            tc_data_type_varchar_nm                                                                             AS tc_data_type_varchar_nm,
            tc_data_type_nvarchar_dsc                                                                           AS tc_data_type_nvarchar_dsc,
            tc_data_type_integer_rto                                                                            AS tc_data_type_integer_rto,
            tc_data_type_smallint_nbr                                                                           AS tc_data_type_smallint_nbr,
            tc_data_type_bigint_pct                                                                             AS tc_data_type_bigint_pct,
            tc_data_type_decimal_amt                                                                            AS tc_data_type_decimal_amt,
            tc_data_type_boolean_f                                                                              AS tc_data_type_boolean_f,
            tc_data_type_timestamp_tms                                                                          AS tc_data_type_timestamp_tms,
            tc_data_type_date_dt                                                                                AS tc_data_type_date_dt,
            tc_data_type_time_tm                                                                                AS tc_data_type_time_tm,
            tc_fk_cl_1_key_1                                                                                    AS tc_fk_cl_1_key_1,
            tc_fk_cl_1_key_2                                                                                    AS tc_fk_cl_1_key_2,
            CASE WHEN ( tc_fk_cl_2_key IS NULL ) THEN ( ' ' ) ELSE ( tc_fk_cl_2_key ) END                       AS tc_fk_cl_2_key,
            tc_fk_ev_i_key                                                                                      AS tc_fk_ev_i_key,
            tc_edc_attr_split                                                                                   AS tc_edc_attr_split,
            tc_edc_attr_converge_1                                                                              AS tc_edc_attr_converge_1,
            tc_edc_attr_converge_2                                                                              AS tc_edc_attr_converge_2,
            'DPL|'|| 'CL_CV|' || TRIM(tc_fk_cl_1_key_1)   || '.' || TRIM(tc_fk_cl_1_key_2)                      AS tc_fk_1_cl_sup_key,
            CASE 
                WHEN ( tc_fk_cl_2_key IS NOT NULL ) THEN 'DPL|'|| 'CL_CV|' || 'ST_TP.'  || TRIM(tc_fk_cl_2_key) 
                ELSE
                'DPL|'|| 'CL_CV|' || 'ST_TP.' || 
                CASE WHEN ( tc_fk_cl_2_key IS NULL ) THEN ( '' ) ELSE ( tc_fk_cl_2_key ) END
            END                                                                                                 AS tc_fk_2_cl_sup_key,
            CASE
                WHEN ( tc_fk_ev_i_key IS NOT NULL ) THEN 'DPL|'|| 'EV_TCI_DELTA_C_I|' || TRIM(tc_fk_ev_i_key) 
                ELSE 
                'DPL|'|| 'EV_TCI_DELTA_C_I|' ||
                CASE WHEN ( tc_fk_ev_i_key IS NULL ) THEN ( '' ) ELSE ( tc_fk_ev_i_key ) END      
            END                                                                                                 AS tc_fk_ev_i_sup_key,
            substr(tc_edc_attr_split,1,(instr(tc_edc_attr_split, ' ', 1, 1) - 1))                               AS tc_split_attr_1_edc,
            substr(tc_edc_attr_split,instr(tc_edc_attr_split, ' ', 1, 2 - 1) + 1,
            (instr(tc_edc_attr_split, ' ', 1, 2) - instr(tc_edc_attr_split, ' ', 1, 2 - 1)) - 1)                AS tc_split_attr_2_edc,
            tc_edc_attr_converge_1|| ' , ' || tc_edc_attr_converge_2                                            AS tc_converge_attr_edc
        FROM
            (
                SELECT
                    src_instn_id,
                    tc_exclude_field,
                    tc_obj_instance_key_1,
                    tc_obj_instance_key_2,
                    tc_obj_instance_key_3,
                    tc_data_type_char_ind,
                    tc_data_type_nchar_cd,
                    tc_data_type_varchar_nm,
                    tc_data_type_nvarchar_dsc,
                    tc_data_type_integer_rto,
                    tc_data_type_smallint_nbr,
                    tc_data_type_bigint_pct,
                    tc_data_type_decimal_amt,
                    tc_data_type_boolean_f,
                    tc_data_type_timestamp_tms,
                    tc_data_type_date_dt,
                    tc_data_type_time_tm,
                    tc_fk_cl_1_key_1,
                    tc_fk_cl_1_key_2,
                    tc_fk_cl_2_key,
                    tc_fk_ev_i_key,
                    tc_edc_attr_split,
                    tc_edc_attr_converge_1,
                    tc_edc_attr_converge_2
                FROM
                    (
                        SELECT
                            instn.src_instn_id,
                            src.tc_exclude_field,
                            src.tc_obj_instance_key_1,
                            src.tc_obj_instance_key_2,
                            src.tc_obj_instance_key_3,
                            src.tc_data_type_char_ind,
                            src.tc_data_type_nchar_cd,
                            src.tc_data_type_varchar_nm,
                            src.tc_data_type_nvarchar_dsc,
                            src.tc_data_type_integer_rto,
                            src.tc_data_type_smallint_nbr,
                            src.tc_data_type_bigint_pct,
                            src.tc_data_type_decimal_amt,
                            src.tc_data_type_boolean_f,
                            src.tc_data_type_timestamp_tms,
                            src.tc_data_type_date_dt,
                            src.tc_data_type_time_tm  AS tc_data_type_time_tm,
                            src.tc_fk_cl_1_key_1,
                            src.tc_fk_cl_1_key_2,
                            src.tc_fk_cl_2_key,
                            src.tc_fk_ev_i_key,
                            src.tc_edc_attr_split,
                            src.tc_edc_attr_converge_1,
                            src.tc_edc_attr_converge_2
                        FROM
                                {{ source('pms_stg', 'PMS_IS_STG_IP_TCV_FULL_CUD_V') }} src
-- Get the instance ID of the last successful interface load for the processing period
                            CROSS JOIN (
                                SELECT
                                    MAX(src_instn_id) AS src_instn_id
                                FROM
                                    {{ source('pms_ctl', 'CTL_SRC_INSTN_LOG') }}
                                WHERE
                                        src_nm = 'EXA_FULL_CU'
                                    AND src_instn_tms = {{ parse_datetime(var("pm_pcs_period")) }}
                            ) instn
                    )
            ) balop_1
