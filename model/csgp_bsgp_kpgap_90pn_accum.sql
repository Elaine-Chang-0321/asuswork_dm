{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    unique_key=['product_line', 'period', 'part_number', 'ds'],
    partitioned_by=['ds'],
    write_compression= 'GZIP',
    incremental_strategy='merge',
    tags= ['ads_dev'],
    table_properties= {
        'vacuum_max_snapshot_age_seconds': '1'
    },    
    pre_hook  = [ "{% if is_incremental() %} delete from {{this}} where ds = '{{var('ds')}}'; {% endif %}" ],
    meta= {
        'job': "csg;csgp_bsgp_kpgap_90pn_accum",
        'alias': "PL, category, ag_no, period 及各個價差 用90展開",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_bsgp_kpgap_90pn_accum' 
)}}
WITH accum_union_missed AS
(
    SELECT 
        product_line,
        category,
        ag_no,
        NULL AS item_no,
        period,
        equo_price,
        cogs_price,
        bs_gap
    FROM {{ source('ads', 'csgp_bsgp_kpgap_accum') }} AS kpgap_accum
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_kpgap_accum') }})
    UNION
    SELECT DISTINCT product_line,
        category,
        NULL AS ag_no,
        item_no,
        gl_yearmonth AS period,
        CAST(equo_price AS DECIMAL(38,15)) AS equo_price,
        CAST(cogs_price AS DECIMAL(38,15)) AS cogs_price,
        CAST(bs_gap AS DECIMAL(38,15)) AS bs_gap
    FROM {{ source('ods', 's_apza002bid_csgp_bsgp_ag_no_missed') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_bsgp_ag_no_missed') }})
),
itemno_desc_pl AS
(
    SELECT *
    , SUBSTR(part_number, 3, 2) AS product_line
    FROM  {{ source('ads', 'csgp_bsgp_agno_itemno_desc')}}
    WHERE ds = (SELECT MAX(ds) FROM  {{ source('ads', 'csgp_bsgp_agno_itemno_desc')}})
),
join_base AS (
    SELECT 
        kpgap_accum.product_line,
        kpgap_accum.category,
        kpgap_accum.ag_no,
        kpgap_accum.item_no,
        kpgap_accum.period,
        kpgap_accum.equo_price,
        kpgap_accum.cogs_price,
        kpgap_accum.bs_gap,
        COALESCE(agno_mapping.part_number, itemno_mapping.part_number) AS part_number,
        COALESCE(agno_mapping.kp_qty, itemno_mapping.kp_qty, 0) AS kp_qty
    FROM accum_union_missed AS kpgap_accum
    LEFT JOIN (
        SELECT DISTINCT product_line, ag_no, part_number, kp_qty
        FROM itemno_desc_pl
    )AS agno_mapping
    ON kpgap_accum.product_line = agno_mapping.product_line AND kpgap_accum.ag_no = agno_mapping.ag_no
    LEFT JOIN (
        SELECT DISTINCT product_line, item_no, part_number, kp_qty
        FROM itemno_desc_pl
    )AS itemno_mapping
    ON kpgap_accum.product_line = itemno_mapping.product_line AND kpgap_accum.item_no = itemno_mapping.item_no
)
SELECT
    product_line,
    period,
    part_number,
    SUM(CASE 
        WHEN category = 'LCD' THEN bs_gap * kp_qty 
        ELSE 0 
    END) AS lcd_gap,

    SUM(CASE 
        WHEN category IN ('SSD', 'HDD', 'Flash') THEN bs_gap * kp_qty 
        ELSE 0 
    END) AS ssd_gap,

    SUM(CASE 
        WHEN category = 'DDR' THEN bs_gap * kp_qty 
        ELSE 0 
    END) AS ddr_gap,

    SUM(CASE 
        WHEN category = 'BATT' THEN bs_gap * kp_qty 
        ELSE 0 
    END) AS batt_gap,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM join_base
GROUP BY 
    product_line,
    period,
    part_number