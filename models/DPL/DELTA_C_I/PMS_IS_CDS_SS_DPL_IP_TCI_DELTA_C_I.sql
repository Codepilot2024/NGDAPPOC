{{ config(
    schema=var('PS_DB_IS_ORA_SCM_v0001.$ORA_SCM_CDS') ,
    materialized='incremental'
) }}



         SELECT
            FARM_FINGERPRINT(GENERATE_UUID()) AS IP_TCI_DELTA_C_I_ID,
            IP_TCI_DELTA_C_I_SUP_KEY AS IP_TCI_DELTA_C_I_SUP_KEY,
            MSTR_SRC_STM_CD AS MSTR_SRC_STM_CD,
            ENT_TP_CD AS ENT_TP_CD,
            MSTR_SRC_STM_KEY AS MSTR_SRC_STM_KEY,
            SRC_STM_CD AS SRC_STM_CD,
            VLD_FROM_TMS AS VLD_FROM_TMS,
            {{ parse_datetime('9999-12-31 00:00:00')}} AS VLD_TO_TMS,
            {{ var("pm_pcs_id") }} AS PCS_ISRT_ID,
            CURRENT_DATETIME AS ISRT_TMS,
            SRC_INSTN_ISRT_ID AS SRC_INSTN_ISRT_ID,
            TC_OBJ_INSTANCE_KEY_1 AS TC_OBJ_INSTANCE_KEY_1,
            TC_OBJ_INSTANCE_KEY_2 AS TC_OBJ_INSTANCE_KEY_2,
            TC_OBJ_INSTANCE_KEY_3 AS TC_OBJ_INSTANCE_KEY_3,
            TC_DATA_TYPE_CHAR_IND_CHANGED_NAME AS TC_DATA_TYPE_CHAR_IND_CHANGED_NAME,
            TC_DATA_TYPE_NCHAR_CD AS TC_DATA_TYPE_NCHAR_CD,
            TC_DATA_TYPE_VARCHAR_NM AS TC_DATA_TYPE_VARCHAR_NM,
            TC_DATA_TYPE_NVARCHAR_DSC AS TC_DATA_TYPE_NVARCHAR_DSC,
            TC_DATA_TYPE_INTEGER_RTO AS TC_DATA_TYPE_INTEGER_RTO,
            TC_DATA_TYPE_SMALLINT_NBR AS TC_DATA_TYPE_SMALLINT_NBR,
            TC_DATA_TYPE_BIGINT_PCT AS TC_DATA_TYPE_BIGINT_PCT,
            TC_DATA_TYPE_DECIMAL_AMT AS TC_DATA_TYPE_DECIMAL_AMT,
            TC_DATA_TYPE_BOOLEAN_F AS TC_DATA_TYPE_BOOLEAN_F,
            TC_DATA_TYPE_TIMESTAMP_TMS AS TC_DATA_TYPE_TIMESTAMP_TMS,
            TC_DATA_TYPE_DATE_DT AS TC_DATA_TYPE_DATE_DT,
            TC_DATA_TYPE_TIME_TM AS TC_DATA_TYPE_TIME_TM,
            TC_FK_CL_1_KEY_1 AS TC_FK_CL_1_KEY_1,
            TC_FK_CL_1_KEY_2 AS TC_FK_CL_1_KEY_2,
            TC_FK_CL_2_KEY AS TC_FK_CL_2_KEY,
            TC_EDC_ATTR_SPLIT AS TC_EDC_ATTR_SPLIT,
            TC_EDC_ATTR_CONVERGE_1 AS TC_EDC_ATTR_CONVERGE_1,
            TC_EDC_ATTR_CONVERGE_2 AS TC_EDC_ATTR_CONVERGE_2,
            TC_FK_1_CL_ID AS TC_FK_1_CL_ID,
            TC_FK_2_CL_ID AS TC_FK_2_CL_ID,
            TC_SPLIT_ATTR_1_EDC AS TC_SPLIT_ATTR_1_EDC,
            TC_SPLIT_ATTR_2_EDC AS TC_SPLIT_ATTR_2_EDC,
            TC_CONVERGE_ATTR_EDC AS TC_CONVERGE_ATTR_EDC
        FROM
            (
                SELECT
                    IP_TCI_DELTA_C_I_SUP_KEY,
                    MSTR_SRC_STM_CD,
                    ENT_TP_CD,
                    MSTR_SRC_STM_KEY,
                    SRC_STM_CD,
                    VLD_FROM_TMS,
                    SRC_INSTN_ISRT_ID,
                    TC_OBJ_INSTANCE_KEY_1,
                    TC_OBJ_INSTANCE_KEY_2,
                    TC_OBJ_INSTANCE_KEY_3,
                    TC_DATA_TYPE_CHAR_IND_CHANGED_NAME,
                    TC_DATA_TYPE_NCHAR_CD,
                    TC_DATA_TYPE_VARCHAR_NM,
                    TC_DATA_TYPE_NVARCHAR_DSC,
                    TC_DATA_TYPE_INTEGER_RTO,
                    TC_DATA_TYPE_SMALLINT_NBR,
                    TC_DATA_TYPE_BIGINT_PCT,
                    TC_DATA_TYPE_DECIMAL_AMT,
                    TC_DATA_TYPE_BOOLEAN_F,
                    TC_DATA_TYPE_TIMESTAMP_TMS,
                    TC_DATA_TYPE_DATE_DT,
                    TC_DATA_TYPE_TIME_TM,
                    TC_FK_CL_1_KEY_1,
                    TC_FK_CL_1_KEY_2,
                    TC_FK_CL_2_KEY,
                    TC_EDC_ATTR_SPLIT,
                    TC_EDC_ATTR_CONVERGE_1,
                    TC_EDC_ATTR_CONVERGE_2,
                    TC_FK_1_CL_ID,
                    TC_FK_2_CL_ID,
                    TC_SPLIT_ATTR_1_EDC,
                    TC_SPLIT_ATTR_2_EDC,
                    TC_CONVERGE_ATTR_EDC
                FROM
                    (
                        SELECT
                            IP_TCI_DELTA_C_I_SUP_KEY,
                            S.MSTR_SRC_STM_CD,
                            S.ENT_TP_CD,
                            S.MSTR_SRC_STM_KEY,
                            S.SRC_STM_CD,
                            S.VLD_FROM_TMS,
                            S.SRC_INSTN_ISRT_ID,
                            S.TC_OBJ_INSTANCE_KEY_1,
                            S.TC_OBJ_INSTANCE_KEY_2,
                            S.TC_OBJ_INSTANCE_KEY_3,
                            S.TC_DATA_TYPE_CHAR_IND_CHANGED_NAME,
                            S.TC_DATA_TYPE_NCHAR_CD,
                            S.TC_DATA_TYPE_VARCHAR_NM,
                            S.TC_DATA_TYPE_NVARCHAR_DSC,
                            S.TC_DATA_TYPE_INTEGER_RTO,
                            S.TC_DATA_TYPE_SMALLINT_NBR,
                            S.TC_DATA_TYPE_BIGINT_PCT,
                            S.TC_DATA_TYPE_DECIMAL_AMT,
                            S.TC_DATA_TYPE_BOOLEAN_F,
                            S.TC_DATA_TYPE_TIMESTAMP_TMS,
                            S.TC_DATA_TYPE_DATE_DT,
                            S.TC_DATA_TYPE_TIME_TM  AS TC_DATA_TYPE_TIME_TM,
                            S.TC_FK_CL_1_KEY_1,
                            S.TC_FK_CL_1_KEY_2,
                            S.TC_FK_CL_2_KEY,
                            S.TC_EDC_ATTR_SPLIT,
                            S.TC_EDC_ATTR_CONVERGE_1,
                            S.TC_EDC_ATTR_CONVERGE_2,
                            LK_TC_FK_1_CL_SUP_KEY.CL_ID AS TC_FK_1_CL_ID,
                            LK_TC_FK_2_CL_SUP_KEY.CL_ID AS TC_FK_2_CL_ID,
                            S.TC_SPLIT_ATTR_1_EDC,
                            S.TC_SPLIT_ATTR_2_EDC,
                            S.TC_CONVERGE_ATTR_EDC
                        FROM
                            {{ ref('PMS_IS_SAC_DPL_SS_DPL_IP_TCI_DELTA_C_I') }} S
                            INNER JOIN {{ source('pms_cds_key','PMS_IS_CDS_CL_K') }} LK_TC_FK_1_CL_SUP_KEY ON (S.TC_FK_1_CL_SUP_KEY = LK_TC_FK_1_CL_SUP_KEY.CL_SUP_KEY)
                            LEFT OUTER JOIN {{ source('pms_cds_key','PMS_IS_CDS_CL_K') }} LK_TC_FK_2_CL_SUP_KEY ON (S.TC_FK_2_CL_SUP_KEY = LK_TC_FK_2_CL_SUP_KEY.CL_SUP_KEY)
                    ) D
            ) BALOP_1

    {% if is_incremental() %}
    where VLD_FROM_TMS > (select coalesce(max(VLD_FROM_TMS), {{ parse_datetime('1900-01-01 00:00:00') }} ) from {{ this }} )
    {% endif %}