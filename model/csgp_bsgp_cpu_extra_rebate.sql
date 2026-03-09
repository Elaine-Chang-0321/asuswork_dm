{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    unique_key=['product_line', 'period', 'part_number', 'ds'], 
    incremental_strategy='merge',
    partitioned_by=['ds'],
    write_compression= 'GZIP',
    tags= ['ads_dev'],
    table_properties= {
        'vacuum_max_snapshot_age_seconds': '1'
    },    
    pre_hook  = [ "{% if is_incremental() %} delete from {{this}} where ds = '{{var('ds')}}'; {% endif %}" ],
    meta= {
        'job': "csgp_bsgp_cpu_extra_rebate",
        'alias': "PL, cpu, extra_rebate, period, part_number, model_name",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_bsgp_cpu_extra_rebate' 
)}}


With gp_kp_result AS (
    SELECT DISTINCT
        product_line,
        part_number,
        cpu,
        model_name
    FROM {{ source('ads', 'mapping_keypart_result_gp') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'mapping_keypart_result_gp') }})
),
csgp_extra_rebate AS (
    SELECT DISTINCT
        item_no,
        product_line,
        cpu,
        extra_rebate,
        period
    FROM {{ source('ods', 's_apza002bid_csgp_extra_rebate') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_extra_rebate') }})
),
group_item_no AS (
    SELECT DISTINCT
        item_no,
        part_number
    FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }})
)

SELECT DISTINCT
    gp_kp_result.product_line,
    reb_pn.cpu,
    reb_pn.extra_rebate,
    reb_pn.period,
    gp_kp_result.part_number,
    gp_kp_result.model_name,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM gp_kp_result
LEFT JOIN group_item_no ON gp_kp_result.part_number = group_item_no.part_number
LEFT JOIN csgp_extra_rebate AS reb_pn 
    ON group_item_no.item_no = reb_pn.item_no 
    AND gp_kp_result.product_line = reb_pn.product_line
WHERE 
    reb_pn.extra_rebate IS NOT NULL
    AND (
        -- 如果是 NV 產品線且 CPU 是 Z1 Extreme，則只匹配 model_name = RC72LA
        NOT (
            gp_kp_result.product_line = 'NV' 
            AND UPPER(reb_pn.cpu) = 'Z1 EXTREME'
        )
        OR gp_kp_result.model_name = 'RC72LA'
    )