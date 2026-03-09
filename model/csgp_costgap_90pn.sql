{{
    config( 
        format='parquet',
        materialized= 'incremental',      
        incremental_strategy='append',                  
        partitioned_by= ['ds'],
        table_type= 'iceberg',
        write_compression= 'GZIP',
        tags= ['ads_dev'],
        table_properties= {
            'vacuum_max_snapshot_age_seconds': '1'
        },    
        pre_hook  = [ "{% if is_incremental() %} delete from {{this}} where ds = '{{var('ds')}}'; {% endif %}" ],
        meta= {
            'job': "csgp_costgap_90pn",
            'alias': "PL, period, part_number 及各個價差",
        },
        location = 's3://consumer-npspo/Tables/ads_dev/csgp_costgap_90pn'        
    )
}}
WITH source_kpgap AS (
    SELECT
        product_line,
        period,
        part_number,
        lcd_gap,
        ssd_gap,
        ddr_gap,
        batt_gap
    FROM {{ source('ads', 'csgp_bsgp_kpgap_90pn_accum') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_kpgap_90pn_accum') }})
),
source_cpugpu AS
(
    SELECT
        product_line,
        period,
        part_number,
        cpu_gap,
        cpu_rebate,
        gpu_gap,
        gpu_rebate
    FROM {{ source('ads', 'csgp_bsgp_kpgap_cpugpu_90pn_accum') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_kpgap_cpugpu_90pn_accum') }})
),
source_nv_sellout AS
(
    SELECT
        product_line,
        period,
        part_number,
        reb AS rebate
    FROM {{ source('ads', 'csgp_kpgp_nv_sellout_rebate') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_kpgp_nv_sellout_rebate') }})
),
source_cpu_extra AS
(
    SELECT
        product_line,
        period,
        part_number,
        extra_rebate
    FROM {{  source('ads', 'csgp_bsgp_cpu_extra_rebate') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_bsgp_cpu_extra_rebate') }})
),
source_opp AS
(
    SELECT
        product_line,
        period,
        sku90pn AS part_number,
        opp_rebate
    FROM {{ source('ods','s_apza002bid_csgp_opp') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods','s_apza002bid_csgp_opp') }})
),
skeleton AS (
    SELECT product_line, period, part_number FROM source_kpgap
    UNION  -- 使用 UNION 會自動去重
    SELECT product_line, period, part_number FROM source_cpugpu
    UNION
    SELECT product_line, period, part_number FROM source_nv_sellout
    UNION
    SELECT product_line, period, part_number FROM source_cpu_extra
    UNION
    SELECT product_line, period, part_number FROM source_opp
)
SELECT
    s.product_line,
    s.period,
    s.part_number,
    COALESCE(kpgap.lcd_gap, 0) AS lcd_gap,
    COALESCE(kpgap.ssd_gap, 0) AS ssd_gap,
    COALESCE(kpgap.ddr_gap, 0) AS ddr_gap,
    COALESCE(kpgap.batt_gap, 0) AS batt_gap,
    COALESCE(cpugpu.cpu_gap, 0) AS cpu_gap,
    COALESCE(cpugpu.cpu_rebate, 0) AS cpu_rebate,
    COALESCE(cpugpu.gpu_gap, 0) AS gpu_gap,
    COALESCE(cpugpu.gpu_rebate, 0) AS gpu_rebate,
    COALESCE(CAST(nv_sellout.rebate AS decimal(34, 15)), 0) AS nv_rebate,
    COALESCE(CAST(cpu_extra.extra_rebate AS decimal(34, 15)), 0) AS cpu_extra_rebate,
    COALESCE(CAST(opp.opp_rebate AS decimal(34, 15)), 0) AS opp_rebate,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM skeleton s
LEFT JOIN source_kpgap AS kpgap
ON s.product_line = kpgap.product_line
AND s.period = kpgap.period
AND s.part_number = kpgap.part_number
LEFT JOIN source_cpugpu AS cpugpu
ON s.product_line = cpugpu.product_line
AND s.period = cpugpu.period
AND s.part_number = cpugpu.part_number
LEFT JOIN source_nv_sellout AS nv_sellout
ON s.product_line = nv_sellout.product_line
AND s.period = nv_sellout.period
AND s.part_number = nv_sellout.part_number
LEFT JOIN source_cpu_extra AS cpu_extra
ON s.product_line = cpu_extra.product_line
AND s.period = cpu_extra.period
AND s.part_number = cpu_extra.part_number
LEFT JOIN source_opp AS opp
ON s.product_line = opp.product_line
AND s.period = opp.period
AND s.part_number = opp.part_number