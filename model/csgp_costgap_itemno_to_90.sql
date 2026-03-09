
{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    unique_key=['item_no', 'ds'],
    partitioned_by=['ds'],
    write_compression= 'GZIP',
    incremental_strategy='merge',
    tags= ['ads_dev'],
    table_properties= {
        'vacuum_max_snapshot_age_seconds': '1'
    },    
    pre_hook  = [ "{% if is_incremental() %} delete from {{this}} where ds = '{{var('ds')}}'; {% endif %}" ],
    meta= {
        'job': "csg;csgp_costgap_itemno_to_90",
        'alias': "把需拿去展90的item_no彙整",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_costgap_itemno_to_90' 
)}}
WITH accum AS (
    SELECT *
    FROM {{ source('ads', 'csgp_bsgp_kpgap_accum') }} accum
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_kpgap_accum') }})
),
cpugpu_accum AS (
    SELECT *
    FROM {{ source('ads', 'csgp_bsgp_kpgap_cpugpu_accum') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_kpgap_cpugpu_accum') }})
),
item_desc AS (
    SELECT *
    FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_agno_itemno_desc') }})
),
os_incentive AS (
    SELECT distinct os_part_number
    FROM {{ source('ods', 's_apza002bid_csgp_os_incentive') }} 
    WHERE ds = (SELECT MAX(ds) FROM  {{ source('ods', 's_apza002bid_csgp_os_incentive') }} )
)

SELECT item_desc.item_no, {{get_current_time()}} AS dw_ins_time, '{{var('ds')}}' AS ds
FROM accum
LEFT JOIN item_desc
ON accum.ag_no = item_desc.ag_no
UNION 
SELECT item_no, {{get_current_time()}} AS dw_ins_time, '{{var('ds')}}' AS ds
FROM cpugpu_accum
UNION
SELECT os_part_number, {{get_current_time()}} AS dw_ins_time, '{{var('ds')}}' AS ds
FROM os_incentive