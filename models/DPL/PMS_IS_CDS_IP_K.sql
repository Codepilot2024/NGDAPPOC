{{ config(
    schema=var('PS_DB_IS_ORA_SCM_v0001.$ORA_SCM_CDS') ,
    materialized='incremental'
) }}


                    SELECT
                        
						GENERATE_UUID() AS IP_ID,
                        IP_SUP_KEY AS IP_SUP_KEY,
                        MSTR_SRC_STM_CD AS MSTR_SRC_STM_CD,
                        ENT_TP_CD AS ENT_TP_CD,
                        MSTR_SRC_STM_KEY AS MSTR_SRC_STM_KEY,
                        SRC_STM_CD AS SRC_STM_CD,
						{{ var("pm_pcs_id") }}  AS PCS_ISRT_ID,
                        CURRENT_DATETIME AS ISRT_TMS,
                        SRC_INSTN_ISRT_ID AS SRC_INSTN_ISRT_ID
                    FROM
                        (
                            SELECT DISTINCT
                                BALOP_1.IP_SUP_KEY AS IP_SUP_KEY,
                                BALOP_1.MSTR_SRC_STM_CD AS MSTR_SRC_STM_CD,
                                BALOP_1.ENT_TP_CD AS ENT_TP_CD,
                                BALOP_1.MSTR_SRC_STM_KEY AS MSTR_SRC_STM_KEY,
                                BALOP_1.SRC_STM_CD AS SRC_STM_CD,
                                BALOP_1.SRC_INSTN_ISRT_ID AS SRC_INSTN_ISRT_ID
                            FROM
                                {{ ref('PMS_IS_SAC_DPL_SS_DPL_IP_TCV_FULL_CUD_V') }} BALOP_1
                                LEFT JOIN {{ this }} BALOP_2
                                 ON (BALOP_1.IP_SUP_KEY = BALOP_2.IP_SUP_KEY)
                            WHERE
                                BALOP_2.IP_ID IS NULL
                        ) BALOP_3