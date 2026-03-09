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


With gp_kp_result AS (
    SELECT DISTINCT
        product_line,
        part_number,
        gpu
    FROM {{ source('ads', 'mapping_keypart_result_gp') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'mapping_keypart_result_gp') }})
),
nv_sellout AS (
    SELECT
        item_no,
        product_line,
        product_number,
        reb,
        period
    FROM {{ source('ods', 's_apza002bid_csgp_nv_sellout') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_nv_sellout') }})

),
group_item_no AS (
    SELECT DISTINCT
        item_no,
        part_number,
        item_desc
    FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }})
)

SELECT DISTINCT
    gp_kp_result.product_line,
    gp_kp_result.part_number,
    gp_kp_result.gpu,
    nv_sellout.reb AS reb,
    nv_sellout.period,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM gp_kp_result
LEFT JOIN group_item_no ON gp_kp_result.part_number = group_item_no.part_number
LEFT JOIN nv_sellout 
    ON group_item_no.item_no = nv_sellout.item_no
    AND gp_kp_result.product_line = nv_sellout.product_line
WHERE nv_sellout.reb IS NOT NULL