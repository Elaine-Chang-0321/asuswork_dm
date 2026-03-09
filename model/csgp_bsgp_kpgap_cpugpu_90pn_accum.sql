{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    partitioned_by=['ds'],
    write_compression= 'GZIP',
    incremental_strategy='append',
    tags= ['ads_dev'],
    table_properties= {
        'vacuum_max_snapshot_age_seconds': '1'
    },
    pre_hook  = [ "{% if is_incremental() %} delete from {{this}} where ds = '{{var('ds')}}'; {% endif %}" ],
    meta= {
        'job': "csg;csgp_bsgp_kpgap_cpugpu_90pn_accum",
        'alias': "PL, category, ag_no, period 及各個價差 用90展開",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_bsgp_kpgap_cpugpu_90pn_accum' 
)}}
WITH itemno_desc_pl AS
(
    SELECT *
    , SUBSTR(part_number, 3, 2) AS product_line
    FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc')}}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }})
),
join_base AS (
    SELECT 
        kpgap_accum.product_line,
        kpgap_accum.category,
        kpgap_accum.item_no,
        kpgap_accum.period,
        kpgap_accum.bs_gap,
        kpgap_accum.rebate,
        itemno_mapping.part_number AS part_number,
        itemno_mapping.kp_qty AS kp_qty
    FROM {{ source('ads', 'csgp_bsgp_kpgap_cpugpu_accum') }} AS kpgap_accum
    LEFT JOIN (
        SELECT DISTINCT product_line, item_no, part_number, kp_qty
        FROM itemno_desc_pl
    )AS itemno_mapping
    ON kpgap_accum.product_line = itemno_mapping.product_line 
    AND kpgap_accum.item_no = itemno_mapping.item_no
    WHERE kpgap_accum.ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_kpgap_cpugpu_accum') }})
)
SELECT
    product_line,
    period,
    part_number,
    SUM(CASE 
        WHEN category = 'CPU' THEN bs_gap * kp_qty 
        ELSE 0 
    END) AS cpu_gap,
    SUM(CASE 
        WHEN category = 'CPU' THEN rebate * kp_qty 
        ELSE 0 
    END) AS cpu_rebate,
    SUM(CASE 
        WHEN category = 'C.S' THEN bs_gap * kp_qty 
        ELSE 0 
    END) AS gpu_gap,
    SUM(CASE 
        WHEN category = 'C.S' THEN rebate * kp_qty 
        ELSE 0 
    END) AS gpu_rebate,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM join_base
GROUP BY 
    product_line,
    period,
    part_number