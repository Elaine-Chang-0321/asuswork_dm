{{ 
    config(
        format='parquet',
        materialized='incremental',
        incremental_strategy='append',
        partitioned_by= ['ds'],
        table_type='iceberg',
        write_compression= 'GZIP',
        pre_hook  = [ "{% if is_incremental() %} delete from {{this}} where ds = '{{var('ds')}}'; {% endif %}" ],  
    )
}}
With mapping_tag AS (
    SELECT 
        asus_pn_desc,
        tag
    FROM {{ source('ods', 's_apza002bid_csgp_bsgp_agno_mappingtag') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_bsgp_agno_mappingtag') }})
),
ag_no_item_desc AS (
    SELECT 
        item_no,
        ag_no,
        item_desc,
        tag
    FROM (
        SELECT 
            a.asus_pn AS item_no,
            a.ag_no,
            a.asus_pn_desc AS item_desc,
            COALESCE(b.tag, '') AS tag,
            ROW_NUMBER() OVER (
                PARTITION BY a.asus_pn 
                ORDER BY 
                    CASE WHEN a.ag_no IS NOT NULL THEN 0 ELSE 1 END,  -- 優先選擇有 ag_no 的記錄
                    a.group_no DESC  -- 再按 group_no 降序
            ) AS rn
        FROM {{ source('ads', 'ads_dl_scm_ems_equo_scm_eqo_all_pn_price_vw_dl') }} a
        LEFT JOIN mapping_tag b ON a.asus_pn_desc LIKE b.asus_pn_desc
        WHERE a.ds = (SELECT MAX(ds) FROM {{ source('ads', 'ads_dl_scm_ems_equo_scm_eqo_all_pn_price_vw_dl') }})
        AND a.project_no IN ('NB', 'NB_JDM', 'NB_ODM')
        AND a.is_odm IN ('N', 'Y', 'J')
        AND a.buying_mode = 'B'
        --AND group_no IN ('202506', '202507', '202508')
    ) ranked
    WHERE rn = 1
),
item_explode_90 AS (
SELECT 
    part_number_kp,
    part_number_90 as part_number,
    SUM(kp_qty) as kp_qty
FROM {{ source('cdm', 'dwd_sc_bu_kp_mapping_90_mid') }} 
WHERE ds = (SELECT MAX(ds) FROM {{ source('cdm', 'dwd_sc_bu_kp_mapping_90_mid') }}) and substitute_type = 'M'
group by part_number_kp,
         part_number_90
)
SELECT
    item_no,
    ag_no,
    item_desc,
    part_number,
    tag,
    kp_qty,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM ag_no_item_desc
LEFT JOIN item_explode_90 on ag_no_item_desc.item_no = item_explode_90.part_number_kp
