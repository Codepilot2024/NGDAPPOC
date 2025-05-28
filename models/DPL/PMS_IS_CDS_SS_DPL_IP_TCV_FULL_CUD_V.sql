{{ config(
    schema=var('PS_DB_IS_ORA_SCM_v0001.$ORA_SCM_CDS') ,
    materialized='incremental',
    incremental_strategy='merge',
    post_hook=
    "MERGE INTO {{ this }} TGT USING
    (
        SELECT
            IP_TCV_FULL_CUD_V_ID,
            IP_ID,
            VLD_TO_TMS,
            PCS_UDT_ID,
            UDT_TMS,
            SRC_INSTN_UDT_ID
        FROM
        (
        select
            IP_TCV_FULL_CUD_V_ID AS IP_TCV_FULL_CUD_V_ID,
            IP_ID AS IP_ID,
            VLD_FROM_TMS AS VLD_TO_TMS,
            {{ var('pm_pcs_id') }}  AS PCS_UDT_ID,
            CURRENT_DATETIME AS UDT_TMS,
            SRC_INSTN_ISRT_ID AS SRC_INSTN_UDT_ID,
            ROW_NUMBER() OVER (PARTITION BY IP_TCV_FULL_CUD_V_ID, IP_ID ORDER BY IP_TCV_FULL_CUD_V_ID, IP_ID) AS RemdupPivotColumn6
        FROM
            {{ ref('PMS_IS_SAC_SS_DPL_IP_TCV_FULL_CUD_V_D') }} 
            WHERE IP_TCV_FULL_CUD_V_ID IS NOT NULL 
        ) BALOP1 WHERE RemdupPivotColumn6=1
        ) SRC ON
        (TGT.IP_TCV_FULL_CUD_V_ID=SRC.IP_TCV_FULL_CUD_V_ID
        AND TGT.IP_ID=SRC.IP_ID AND TGT.VLD_TO_TMS=CAST('9999-12-31 00:00:00' AS DATETIME))
        WHEN MATCHED THEN UPDATE
        SET
        TGT.VLD_TO_TMS = SRC.VLD_TO_TMS, TGT.PCS_UDT_ID = SRC.PCS_UDT_ID, TGT.UDT_TMS = SRC.UDT_TMS, TGT.SRC_INSTN_UDT_ID = SRC.SRC_INSTN_UDT_ID"
) }}



SELECT              DISTINCT
                    IP_ID AS IP_ID,    
                    generate_uuid() as IP_TCV_FULL_CUD_V_ID,
                    IP_SUP_KEY AS IP_SUP_KEY,
                    MSTR_SRC_STM_CD AS MSTR_SRC_STM_CD,
                    ENT_TP_CD AS ENT_TP_CD,
                    MSTR_SRC_STM_KEY AS MSTR_SRC_STM_KEY,
                    SRC_STM_CD AS SRC_STM_CD,
                    HASH_VAL AS HASH_VAL,
                    VLD_FROM_TMS AS VLD_FROM_TMS,
                    CAST('9999-12-31 00:00:00' AS DATETIME) AS VLD_TO_TMS,
                    {{ var("pm_pcs_id") }}  AS PCS_ISRT_ID,
                    CURRENT_DATETIME AS ISRT_TMS,
                    NULL AS PCS_UDT_ID,
                    cast(NULL as datetime)  AS UDT_TMS,
                    NULL AS SRC_INSTN_UDT_ID,
                    DEL_IN_SRC_STM_IND AS DEL_IN_SRC_STM_IND,
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
                    TC_DATA_TYPE_TIME_TM  AS TC_DATA_TYPE_TIME_TM,
                    TC_FK_CL_1_KEY_1 AS TC_FK_CL_1_KEY_1,
                    TC_FK_CL_1_KEY_2 AS TC_FK_CL_1_KEY_2,
                    TC_FK_CL_2_KEY AS TC_FK_CL_2_KEY,
                    TC_FK_EV_I_KEY AS TC_FK_EV_I_KEY,
                    TC_EDC_ATTR_SPLIT AS TC_EDC_ATTR_SPLIT,
                    TC_EDC_ATTR_CONVERGE_1 AS TC_EDC_ATTR_CONVERGE_1,
                    TC_EDC_ATTR_CONVERGE_2 AS TC_EDC_ATTR_CONVERGE_2,
                    TC_FK_1_CL_ID AS TC_FK_1_CL_ID,
                    TC_FK_2_CL_ID AS TC_FK_2_CL_ID,
                    TC_FK_EV_I_ID AS TC_FK_EV_I_ID,
                    TC_SPLIT_ATTR_1_EDC AS TC_SPLIT_ATTR_1_EDC,
                    TC_SPLIT_ATTR_2_EDC AS TC_SPLIT_ATTR_2_EDC,
                    TC_CONVERGE_ATTR_EDC AS TC_CONVERGE_ATTR_EDC
                FROM
                    {{ ref('PMS_IS_SAC_SS_DPL_IP_TCV_FULL_CUD_V_D') }} 
                {% if is_incremental() %}
                where VLD_FROM_TMS > (select coalesce(max(VLD_FROM_TMS),'1900-01-01 00:00:00') from {{ this }} )
                {% endif %}


                    
