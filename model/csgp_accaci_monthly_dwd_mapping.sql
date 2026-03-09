{{ config(
    materialized='incremental',
    table_type='iceberg',
    format='parquet',
    incremental_strategy='append',
    partitioned_by=['ds'],
    write_compression= 'GZIP',
    tags= ['ads_dev'],
    table_properties= {
        'vacuum_max_snapshot_age_seconds': '1'
    },
    pre_hook = [ "{% if is_incremental() %} DELETE FROM {{this}} WHERE ds = '{{var('ds')}}'; {% endif %}" ],
    meta= {
        'job': "csgp_accaci_monthly_dwd_mapping",
        'alias': "acc aci data with various mappings",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_accaci_monthly_dwd_mapping'    
)}}
With accaci_raw_maxds AS (
    SELECT * 
    FROM {{ source('ods', 's_apza002bid_csgp_accaci_local_data_monthly') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_accaci_local_data_monthly') }})
),
max_period_data AS (
    SELECT max(period) AS max_period
    FROM accaci_raw_maxds
),  
accaci_raw AS (
    SELECT * 
    FROM accaci_raw_maxds
    WHERE period = (SELECT max_period FROM max_period_data)
),
------------------------mapping 1------------------------
mapping_keypart AS (
    SELECT part_number,
            model_name,
            segmentation,
            gpu,
            cpu_vendor,
            cpu_platform,
            cpu,
            lcd_code,
            cpu_n,
            acc_segment,
            cte
    FROM {{ source('ads', 'mapping_keypart_result_gp') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'mapping_keypart_result_gp') }} WHERE SUBSTR(ds, 1, 6) = (SELECT max_period FROM max_period_data))
),
------------------------mapping 2------------------------
mapping_costgap AS (
    SELECT part_number,
            period,
            opp_rebate,
            cpu_extra_rebate,
            nv_rebate
    FROM {{ source('ads', 'csgp_costgap_90pn') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'csgp_costgap_90pn') }})
),
------------------------mapping 3------------------------
data_1179_raw AS (
    SELECT *
    , freight_cost * all_qty AS freight_amt
    , SUBSTR(estimate_revenue_date, 1, 6) AS period
    , CASE WHEN territory IN ('NA', 'LATAM') THEN 'ACIRAW' WHEN territory = 'CHINA' THEN 'ACCRAW' END AS source
    FROM {{ source('ads', 'ads_sc_cs_bu_gross_profit_simulation_d') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'ads_sc_cs_bu_gross_profit_simulation_d') }})
),
data_freight_amt AS (
    SELECT part_number, period, SUM(freight_amt)/SUM(all_qty) AS freight_cost_unit
    FROM data_1179_raw
    GROUP BY part_number, period
),
data_acc_hqshipfee_raw AS (
    SELECT product_line
      , CAST (acc_hqshipfee AS decimal(34, 15)) AS acc_hqshipfee
      , period
    FROM {{ source('ods', 's_apza002bid_csgp_acc_hqshipfee') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_acc_hqshipfee') }})
),
------------------------mapping 4------------------------
data_hq_raw AS (
    SELECT product_line
      ,sales_team
      ,model_name AS model
      ,item_number AS part_number
      ,period
      ,cpu AS cpu_sku
      ,CAST(item_cost_usd AS decimal(34, 15)) AS item_cost_usd
    FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost') }})
),
data_hq_raw_ex AS (
    SELECT item_number AS part_number
    , model_name AS model
    , cpu AS cpu_sku
    , period
    , CAST(item_cost_usd AS decimal(34, 15)) AS item_cost_usd
    FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost_ex') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost_ex') }})
),
data_aci_item_cost_raw AS (
    SELECT 
        item AS part_number,
        CAST(item_cost_usd AS decimal(34, 15)) AS item_cost_usd,
        period
    FROM {{ source('ods', 's_apza002bid_csgp_aci_item_cost') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_aci_item_cost')}})
),
data_aci_item_cost_raw_ex AS (
    SELECT 
        item AS part_number,
        CAST(item_cost_usd AS decimal(34, 15)) AS item_cost_usd,
        period
    FROM {{ source('ods', 's_apza002bid_csgp_aci_item_cost_ex') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_aci_item_cost_ex')}})
),
data_hq_model_cpu_branch_raw AS (
    SELECT 
        sales_team,
        model_name AS model,
        cpu AS cpu_sku,
        period,
        CAST(item_cost_usd AS decimal(34, 15)) AS item_cost_usd
    FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost_model_cpu') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost_model_cpu') }})
),
data_hq_model_cpu_branch_raw_ex AS (
    SELECT 
        sales_team,
        model_name AS model,
        cpu AS cpu_sku,
        period,
        CAST(item_cost_usd AS decimal(34, 15)) AS item_cost_usd
    FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost_model_cpu_ex') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_hq_item_cost_model_cpu_ex') }})
),
{{ gen_1179_hq_condition([
    {
        'input_cte': 'data_hq_raw', 
        'prefix': 'data_hq', 
        'suffix': '', 
        'has_source': False
    },
    {
        'input_cte': 'data_hq_raw_ex', 
        'prefix': 'data_hq', 
        'suffix': '_ex', 
        'has_source': False
    },
    {
        'input_cte': 'data_1179_raw', 
        'prefix': 'data_1179', 
        'suffix': '', 
        'has_source': True
    }
]) }},

{{ gen_aci_condition([
    {
        'input_cte': 'data_aci_item_cost_raw', 
        'prefix': 'data_aci_item_cost', 
        'suffix': ''
    },
    {
        'input_cte': 'data_aci_item_cost_raw_ex', 
        'prefix': 'data_aci_item_cost', 
        'suffix': '_ex'
    }
]) }},

{{ gen_hq_model_cpu_branch_condition([
    {
        'input_cte': 'data_hq_model_cpu_branch_raw', 
        'prefix': 'data_hq_model_cpu_branch', 
        'suffix': ''
    },
    {
        'input_cte': 'data_hq_model_cpu_branch_raw_ex', 
        'prefix': 'data_hq_model_cpu_branch', 
        'suffix': '_ex'
    }
]) }},
------------------------------mapping 5: os_incentive---------------------------------
os_incentive_raw AS (
    SELECT *
    FROM (
        SELECT * FROM {{ source('ods', 's_apza002bid_csgp_os_incentive') }} os_incentive
        WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_os_incentive') }})
    )tmp
    LEFT JOIN (
        SELECT distinct part_number_kp, part_number_90 AS part_number
        FROM  {{ source('cdm','dwd_sc_bu_kp_mapping_90_mid')}} item_to_90
        WHERE ds = (SELECT MAX(ds) FROM  {{ source('cdm','dwd_sc_bu_kp_mapping_90_mid')}})
    )AS itemno_mapping
    ON tmp.os_part_number = itemno_mapping.part_number_kp
),
------------------------mapping 6: CSC 資料------------------------
csc_data_raw AS (

    SELECT *
    FROM {{ source('ads', 'ads_dl_ebs_dist_om_xx_om_csbu_percentage') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'ads_dl_ebs_dist_om_xx_om_csbu_percentage') }})
    AND country_code = 'CN'
),
csc_local_mapping AS (
    SELECT *
    FROM {{ source('ods', 's_apza002bid_csgp_csc_table') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_csc_table') }})
),
fx_rate_raw AS (
    SELECT *
    FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_rmb') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_rmb') }})
),

mapping_1179 AS (
    SELECT raw.*
    , mk.model_name AS model
    , mk.segmentation AS segment
    , mk.gpu AS gpu
    , mk.cpu_vendor AS cpu_vendor
    , mk.cpu_platform AS cpu_platform
    , mk.cpu AS cpu_sku
    , mk.lcd_code AS lcd_code
    , mk.cpu_n AS cpu_n
    , mk.acc_segment AS acc_segment
    /* Mapping Costgap 計算 */
    , CASE WHEN raw.all_qty < 0 THEN 0 ELSE mc.opp_rebate * raw.all_qty END AS opp_amount
    , CASE WHEN raw.all_qty < 0 THEN 0 ELSE mc.cpu_extra_rebate * raw.all_qty END AS extra_rebate
    , CASE WHEN raw.all_qty < 0 THEN 0 ELSE mc.nv_rebate * raw.all_qty END AS nv_sellout_rebate

    , CAST(CASE WHEN raw.source = 'ACCRAW' THEN acc_hqshipfee.acc_hqshipfee ELSE NULL END AS decimal(34, 15)) AS ship_fee_hq
    , CAST(CASE WHEN raw.source = 'ACCRAW' THEN acc_hqshipfee.acc_hqshipfee * raw.all_qty ELSE NULL END AS decimal(34, 15)) AS total_ship_fee_hq
    , CAST(CASE WHEN raw.source = 'ACCRAW' THEN CAST(raw.total_ship_fee_company AS decimal(34, 15)) + (acc_hqshipfee.acc_hqshipfee * raw.all_qty) ELSE data_freight_amt.freight_cost_unit * raw.all_qty END AS decimal(34, 15)) AS total_ship_fee
    , CAST(CASE WHEN raw.source = 'ACCRAW' THEN CAST(raw.total_ship_fee_company_fxdiff AS decimal(34, 15)) + (acc_hqshipfee.acc_hqshipfee * raw.all_qty) ELSE data_freight_amt.freight_cost_unit * raw.all_qty END AS decimal(34, 15)) AS total_ship_fee_fxdiff

    -- HQ Standard
    , data_hq_pn.item_cost_usd AS item_cost_usd_hqpn
    , data_hq_pn_max.item_cost_usd AS item_cost_usd_hqpn_max
    , data_hq_model_cpu.item_cost_usd AS item_cost_usd_hqmodelcpu
    , data_hq_model_cpu_max.item_cost_usd AS item_cost_usd_hqmodelcpu_max
    
    -- 1179 Standard (Macro 自動帶出)
    , data_1179_pn.cogs_amount AS cogs_amount_1179pn
    , data_1179_pn_max.cogs_amount AS cogs_amount_1179pn_max
    , data_1179_model_cpu.cogs_amount AS cogs_amount_1179modelcpu
    , data_1179_model_cpu_max.cogs_amount AS cogs_amount_1179modelcpu_max

    -- HQ EX (修正名稱：_ex 放最後)
    , data_hq_pn_ex.item_cost_usd AS item_cost_usd_hqpn_ex
    , data_hq_pn_max_ex.item_cost_usd AS item_cost_usd_hqpn_max_ex
    , data_hq_model_cpu_ex.item_cost_usd AS item_cost_usd_hqmodelcpu_ex
    , data_hq_model_cpu_max_ex.item_cost_usd AS item_cost_usd_hqmodelcpu_max_ex
    
    -- 1179 EX/Fxdiff (Macro 自動帶出)
    , data_1179_pn.cogs_amount_fxdiff AS cogs_amount_1179pn_fxdiff
    , data_1179_pn_max.cogs_amount_fxdiff AS cogs_amount_1179pn_max_fxdiff
    , data_1179_model_cpu.cogs_amount_fxdiff AS cogs_amount_1179modelcpu_fxdiff
    , data_1179_model_cpu_max.cogs_amount_fxdiff AS cogs_amount_1179modelcpu_max_fxdiff

    , aci_cost.item_cost_usd AS aci_cost_item_cost_usd
    , aci_cost_ex.item_cost_usd AS aci_cost_item_cost_usd_ex
    , data_hq_model_cpu_branch.item_cost_usd AS hq_model_cpu_branch_item_cost_usd
    , data_hq_model_cpu_branch_ex.item_cost_usd AS hq_model_cpu_branch_item_cost_usd_ex
    /* -------------------------------------------------------------------------
       HQ ASC 邏輯 (Item Cost Selection)
       ------------------------------------------------------------------------- */
    , CASE 
        WHEN raw.source = 'ACCRAW' AND raw.cost_period != '由新到舊' THEN 
            COALESCE(
                data_hq_pn.item_cost_usd, 
                data_hq_pn_max.item_cost_usd, 
                data_hq_model_cpu.item_cost_usd, 
                data_hq_model_cpu_max.item_cost_usd, 
                data_1179_pn.cogs_amount, 
                data_1179_pn_max.cogs_amount, 
                data_1179_model_cpu.cogs_amount, 
                data_1179_model_cpu_max.cogs_amount
            )
        WHEN (raw.source = 'ACCRAW' AND raw.cost_period = '由新到舊') THEN 
            COALESCE(
                data_hq_pn_max.item_cost_usd, 
                data_hq_model_cpu_max.item_cost_usd, 
                data_1179_pn_max.cogs_amount, 
                data_1179_model_cpu_max.cogs_amount
            )
        WHEN raw.source = 'ACIRAW' THEN 
            COALESCE(
                aci_cost.item_cost_usd, 
                data_hq_pn_max.item_cost_usd, 
                data_hq_model_cpu_branch.item_cost_usd, 
                data_1179_pn_max.cogs_amount, 
                data_1179_model_cpu_max.cogs_amount
            )
      END AS hq_asc

    /* -------------------------------------------------------------------------
       HQ ASC FXDIFF 邏輯
       ------------------------------------------------------------------------- */
    , CASE 
        WHEN raw.source = 'ACCRAW' AND raw.cost_period != '由新到舊' THEN 
            COALESCE(
                data_hq_pn_ex.item_cost_usd, 
                data_hq_pn_max_ex.item_cost_usd, 
                data_hq_model_cpu_ex.item_cost_usd, 
                data_hq_model_cpu_max_ex.item_cost_usd, 
                data_1179_pn.cogs_amount_fxdiff, 
                data_1179_pn_max.cogs_amount_fxdiff, 
                data_1179_model_cpu.cogs_amount_fxdiff, 
                data_1179_model_cpu_max.cogs_amount_fxdiff
            )
        WHEN (raw.source = 'ACCRAW' AND raw.cost_period = '由新到舊') THEN 
            COALESCE(
                data_hq_pn_max_ex.item_cost_usd, 
                data_hq_model_cpu_max_ex.item_cost_usd, 
                data_1179_pn.cogs_amount_fxdiff, 
                data_1179_pn_max.cogs_amount_fxdiff, 
                data_1179_model_cpu.cogs_amount_fxdiff, 
                data_1179_model_cpu_max.cogs_amount_fxdiff
            )
        WHEN raw.source = 'ACIRAW' THEN 
            COALESCE(
                aci_cost_ex.item_cost_usd, 
                data_hq_pn_max_ex.item_cost_usd, 
                data_hq_model_cpu_branch_ex.item_cost_usd, 
                data_1179_pn_max.cogs_amount_fxdiff, 
                data_1179_model_cpu_max.cogs_amount_fxdiff
            )
      END AS hq_asc_fxdiff
    , CASE WHEN raw.all_qty < 0 THEN 0 ELSE CAST(os_incentive.inc AS decimal(34, 15)) * raw.all_qty END AS os_incentive

    , CASE WHEN raw.source = 'ACCRAW' THEN  CAST(csc.reserve_margin AS decimal(34, 15)) 
    WHEN raw.source = 'ACIRAW' AND business_type = 'REFURBISH' THEN CAST(csc_local_refurbish.csc AS decimal(34, 15))
    WHEN raw.source = 'ACIRAW' AND business_type != 'REFURBISH' THEN CAST(csc_local_non_refurbish.csc AS decimal(34, 15)) END AS csc_ratio_processed
    , fx.fx_rate
    , fx.fx_rate_ex
    , mk.cte AS is_cte
    FROM accaci_raw raw
    /* --- Mapping Joins --- */
    LEFT JOIN mapping_keypart mk
        ON raw.part_number = mk.part_number
    LEFT JOIN mapping_costgap mc
        ON raw.part_number = mc.part_number
        AND raw.period = mc.period
    LEFT JOIN data_freight_amt
        ON raw.part_number = data_freight_amt.part_number
        AND raw.period = data_freight_amt.period
    LEFT JOIN data_acc_hqshipfee_raw acc_hqshipfee
        ON  raw.product_line_id = acc_hqshipfee.product_line
        AND raw.period = acc_hqshipfee.period
    /* --- 1179 Tables (注意：補上 _sku，來源使用 mk 欄位) --- */
    LEFT JOIN data_1179_pn_period data_1179_pn
        ON raw.part_number = data_1179_pn.part_number
        AND raw.period = data_1179_pn.period
        AND raw.source = data_1179_pn.source
    LEFT JOIN data_1179_pn_maxperiod data_1179_pn_max
        ON raw.part_number = data_1179_pn_max.part_number
        AND raw.source = data_1179_pn_max.source
    LEFT JOIN data_1179_model_cpu_sku_period data_1179_model_cpu 
        ON mk.model_name = data_1179_model_cpu.model
        AND mk.cpu = data_1179_model_cpu.cpu_sku
        AND raw.period = data_1179_model_cpu.period
        AND raw.source = data_1179_model_cpu.source
    LEFT JOIN data_1179_model_cpu_sku_maxperiod data_1179_model_cpu_max
        ON mk.model_name = data_1179_model_cpu_max.model
        AND mk.cpu = data_1179_model_cpu_max.cpu_sku
        AND raw.source = data_1179_model_cpu_max.source
    
    /* --- HQ Standard Tables (注意：HQ 表沒有 source 欄位) --- */
    LEFT JOIN data_hq_pn_period data_hq_pn
        ON raw.part_number = data_hq_pn.part_number
        AND raw.cost_period = data_hq_pn.period
    LEFT JOIN data_hq_pn_maxperiod data_hq_pn_max
        ON raw.part_number = data_hq_pn_max.part_number
    LEFT JOIN data_hq_model_cpu_sku_period data_hq_model_cpu
        ON mk.model_name = data_hq_model_cpu.model
        AND mk.cpu = data_hq_model_cpu.cpu_sku
        AND raw.cost_period = data_hq_model_cpu.period
    LEFT JOIN data_hq_model_cpu_sku_maxperiod data_hq_model_cpu_max
        ON mk.model_name = data_hq_model_cpu_max.model
        AND mk.cpu = data_hq_model_cpu_max.cpu_sku

    /* --- HQ EX Tables (注意：_ex 在最後) --- */
    LEFT JOIN data_hq_pn_period_ex data_hq_pn_ex
        ON raw.part_number = data_hq_pn_ex.part_number
        AND raw.cost_period = data_hq_pn_ex.period
    LEFT JOIN data_hq_pn_maxperiod_ex data_hq_pn_max_ex
        ON raw.part_number = data_hq_pn_max_ex.part_number
    LEFT JOIN data_hq_model_cpu_sku_period_ex data_hq_model_cpu_ex
        ON mk.model_name = data_hq_model_cpu_ex.model
        AND mk.cpu = data_hq_model_cpu_ex.cpu_sku
        AND raw.cost_period = data_hq_model_cpu_ex.period
    LEFT JOIN data_hq_model_cpu_sku_maxperiod_ex data_hq_model_cpu_max_ex
        ON mk.model_name = data_hq_model_cpu_max_ex.model
        AND mk.cpu = data_hq_model_cpu_max_ex.cpu_sku

    /* --- ACI Tables (注意：用 Macro 產出的名稱 pn_period) --- */
    LEFT JOIN data_aci_item_cost_pn_period aci_cost
        ON raw.part_number = aci_cost.part_number
        AND raw.period = aci_cost.period
    LEFT JOIN data_aci_item_cost_pn_period_ex aci_cost_ex
        ON raw.part_number = aci_cost_ex.part_number
        AND raw.period = aci_cost_ex.period

    /* --- HQ Branch Tables (注意：Join 欄位對齊) --- */
    LEFT JOIN data_hq_model_cpu_branch
        ON mk.model_name = data_hq_model_cpu_branch.model
        AND mk.cpu = data_hq_model_cpu_branch.cpu_sku
        AND raw.branch = data_hq_model_cpu_branch.sales_team
    LEFT JOIN data_hq_model_cpu_branch_ex
        ON mk.model_name = data_hq_model_cpu_branch_ex.model
        AND mk.cpu = data_hq_model_cpu_branch_ex.cpu_sku
        AND raw.branch = data_hq_model_cpu_branch_ex.sales_team

    LEFT JOIN os_incentive_raw os_incentive
        ON raw.part_number = os_incentive.part_number
        AND raw.period = os_incentive.period

    LEFT JOIN csc_data_raw csc
        ON raw.product_line_id = csc.product_line
        AND raw.warranty = csc.warranty
        AND raw.ou = csc.ledger_short_name
        AND raw.period = csc.period
    LEFT JOIN csc_local_mapping csc_local_refurbish
        ON raw.product_line_id = csc_local_refurbish.product_line
        AND raw.business_type = csc_local_refurbish.channel_code
        AND raw.period = csc_local_refurbish.period
    LEFT JOIN csc_local_mapping csc_local_non_refurbish
        ON raw.product_line_id = csc_local_non_refurbish.product_line
        AND raw.business_type || '-' || raw.country_code = csc_local_non_refurbish.channel_code
        AND raw.period = csc_local_non_refurbish.period
    LEFT JOIN fx_rate_raw fx
        ON raw.product_line_id = fx.product_line
        AND raw.period = fx.period
)
SELECT 
source
, product_line_id
, year
, quarter
, month
, period
, sold_to_customer
, customer
, order_type
, business_type
, product_type
, distribution
, distribution_type
, territory
, territory2
, branch
, branch2
, country_id
, country_chinese
, ou
, warranty
, csid
, part_number
, description
, all_qty
, revenue_usd_hedge_rate
, revenue_fxdiff
, net_revenue_usd_agp
, net_revenue_fxdiff
, local_cogs_amount
, local_cogs_amount_fxdiff
, credit_note_hedge_rate
, credit_note_fxdiff
, total_ship_fee_company
, total_ship_fee_company_fxdiff
, cost_period
, funding
, csc_ratio_processed AS csc_ratio
, CASE WHEN source = 'ACCRAW' 
            THEN  (CAST(revenue_rmb AS decimal(34, 15)) * csc_ratio_processed)/CAST(fx_rate AS decimal(34, 15)) 
        ELSE CAST(revenue_usd_hedge_rate AS decimal(34, 15)) * csc_ratio_processed END AS csc_amt
, CASE WHEN source = 'ACCRAW' 
            THEN  (CAST(revenue_rmb AS decimal(34, 15)) * csc_ratio_processed)/CAST(fx_rate_ex AS decimal(34, 15)) 
        ELSE CAST(revenue_fxdiff AS decimal(34, 15)) * csc_ratio_processed END AS csc_amt_fxdiff
, ship_fee_hq
, total_ship_fee_hq
, total_ship_fee
, total_ship_fee_fxdiff

-- HQ Standard
, item_cost_usd_hqpn
, item_cost_usd_hqpn_max
, item_cost_usd_hqmodelcpu
, item_cost_usd_hqmodelcpu_max

-- 1179 Standard (Macro 自動帶出)
, cogs_amount_1179pn
, cogs_amount_1179pn_max
, cogs_amount_1179modelcpu
, cogs_amount_1179modelcpu_max

-- HQ EX (修正名稱：_ex 放最後)
, item_cost_usd_hqpn_ex
, item_cost_usd_hqpn_max_ex
, item_cost_usd_hqmodelcpu_ex
, item_cost_usd_hqmodelcpu_max_ex

-- 1179 EX/Fxdiff (Macro 自動帶出)
, cogs_amount_1179pn_fxdiff
, cogs_amount_1179pn_max_fxdiff
, cogs_amount_1179modelcpu_fxdiff
, cogs_amount_1179modelcpu_max_fxdiff

, aci_cost_item_cost_usd
, aci_cost_item_cost_usd_ex
, hq_model_cpu_branch_item_cost_usd
, hq_model_cpu_branch_item_cost_usd_ex

, hq_asc
, hq_asc_fxdiff
, hq_asc * all_qty AS cogs_amount
, hq_asc_fxdiff * all_qty AS cogs_amount_fxdiff
, opp_amount
, extra_rebate
, nv_sellout_rebate
, os_incentive
, model
, segment
, gpu
, cpu_vendor
, cpu_platform
, cpu_sku
, lcd_code
, cpu_n
, acc_segment
, revenue_rmb
, net_revenue_rmb
, local_cogs_amount_rmb
, credit_note_rmb
, total_ship_fee_company_rmb
, CASE WHEN source = 'ACCRAW' 
        THEN  (CAST(revenue_rmb AS decimal(34, 15)) * csc_ratio_processed) ELSE NULL END AS csc_amt_rmb
, is_cte
, {{get_current_time()}} AS dw_ins_time
, '{{var('ds')}}' AS ds
FROM mapping_1179