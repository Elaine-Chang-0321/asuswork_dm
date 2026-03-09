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
        'job': "csgp_accaci_weekly_accum",
        'alias': "acc aci data with various mappings",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_accaci_weekly_accum'    
)}}
-- 从 ads_dev.csgp_accaci_weekly_dwd_mapping 读取数据并进行多层计算

WITH 
fx_rate_raw AS (
    SELECT 
        product_line,
        period,
        fx_rate
    FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_rmb') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_rmb') }})
),
-- 计算1: 基础GP计算
calc_1_base_gp_calculation AS (
    SELECT 
        *,
        -- local_gp 计算
        COALESCE(CAST(revenue_usd_hedge_rate AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(credit_note_hedge_rate AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(local_cogs_amount AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(total_ship_fee_company AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(csc_amt AS DECIMAL(34, 15)), 0) AS local_gp,
        
        -- local_gp_fxdiff 计算
        COALESCE(CAST(revenue_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(credit_note_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(local_cogs_amount_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(total_ship_fee_company_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(csc_amt_fxdiff AS DECIMAL(34, 15)), 0) AS local_gp_fxdiff,
        
        -- gp_pod 计算
        COALESCE(CAST(net_revenue_usd_agp AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(cogs_amount AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(total_ship_fee AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(csc_amt AS DECIMAL(34, 15)), 0) AS gp_pod,
        
        -- gp_pod_fxdiff 计算
        COALESCE(CAST(net_revenue_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(cogs_amount_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(total_ship_fee_fxdiff AS DECIMAL(34, 15)), 0) 
            - COALESCE(CAST(csc_amt_fxdiff AS DECIMAL(34, 15)), 0) AS gp_pod_fxdiff
    FROM {{ source('ads_dev', 'csgp_accaci_weekly_dwd_mapping') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads_dev', 'csgp_accaci_weekly_dwd_mapping') }})
),

-- 計算2: HQ GP 和 GP Net 計算
calc_2_hq_gp_and_gp_net_calculation AS (
    SELECT 
        *,
        -- hq_gp 計算
        gp_pod - local_gp AS hq_gp,
        
        -- hq_gp_usd_fxdiff 計算
        gp_pod_fxdiff - local_gp_fxdiff AS hq_gp_usd_fxdiff,
        
        -- gp_net 計算
        gp_pod 
            + COALESCE(CAST(extra_rebate AS DECIMAL(34, 15)), 0) 
            + COALESCE(CAST(nv_sellout_rebate AS DECIMAL(34, 15)), 0) 
            + COALESCE(CAST(funding AS DECIMAL(34, 15)), 0) 
            + CASE WHEN product_line_id != 'NV' THEN COALESCE(CAST(os_incentive AS DECIMAL(34, 15)), 0) 
            ELSE 0 END AS gp_net,
        
        -- gp_net_fxdiff 計算
        gp_pod_fxdiff 
            + COALESCE(CAST(extra_rebate AS DECIMAL(34, 15)), 0) 
            + COALESCE(CAST(nv_sellout_rebate AS DECIMAL(34, 15)), 0) 
            + COALESCE(CAST(funding AS DECIMAL(34, 15)), 0) 
            + CASE WHEN product_line_id != 'NV' THEN COALESCE(CAST(os_incentive AS DECIMAL(34, 15)), 0) 
            ELSE 0 END AS gp_net_fxdiff
    FROM calc_1_base_gp_calculation
),

-- 計算3: HQ GP Unit 計算
calc_3_hq_gp_unit_calculation AS (
    SELECT 
        *,
        -- hq_gp_unit 計算 (避免除以0)
        CASE 
            WHEN all_qty = 0 OR all_qty IS NULL THEN CAST(0 AS DECIMAL(34, 15))
            ELSE hq_gp / CAST(all_qty AS DECIMAL(34, 15))
        END AS hq_gp_unit,
        
        -- hq_gp_unit_fxdiff 計算 (避免除以0)
        CASE 
            WHEN all_qty = 0 OR all_qty IS NULL THEN CAST(0 AS DECIMAL(34, 15))
            ELSE hq_gp_usd_fxdiff / CAST(all_qty AS DECIMAL(34, 15))
        END AS hq_gp_unit_fxdiff
    FROM calc_2_hq_gp_and_gp_net_calculation
),

-- 計算4: HQ ASP 計算
calc_4_hq_asp_calculation AS (
    SELECT 
        *,
        -- hq_asp 計算
        COALESCE(CAST(hq_asc AS DECIMAL(34, 15)), 0) + hq_gp_unit AS hq_asp,
        
        -- hq_asp_fxdiff 計算
        COALESCE(CAST(hq_asc_fxdiff AS DECIMAL(34, 15)), 0) + hq_gp_unit_fxdiff AS hq_asp_fxdiff
    FROM calc_3_hq_gp_unit_calculation
),

-- 計算5: HQ Revenue 計算
calc_5_hq_revenue_calculation AS (
    SELECT 
        *,
        -- hq_rev 計算
        hq_asp * COALESCE(CAST(all_qty AS DECIMAL(34, 15)), 0) AS hq_rev,
        
        -- hq_rev_usd_fxdiff 計算
        hq_asp_fxdiff * COALESCE(CAST(all_qty AS DECIMAL(34, 15)), 0) AS hq_rev_usd_fxdiff
    FROM calc_4_hq_asp_calculation
),

-- 計算6: RMB換算
calc_6_rmb_conversion AS (
    SELECT
        c5.*,
        fx.fx_rate,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.total_ship_fee_company_rmb AS DECIMAL(34, 15)), 0) + (COALESCE(CAST(c5.total_ship_fee_hq AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0))
            ELSE NULL
        END AS total_ship_fee_rmb,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.cogs_amount AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS cogs_amount_rmb,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.opp_amount AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS opp_amount_rmb,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.extra_rebate AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS extra_rebate_rmb,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.nv_sellout_rebate AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS nv_sellout_rebate_rmb,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.funding AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS funding_rmb,
        CASE
            WHEN c5.source = 'ACCRAW' THEN COALESCE(CAST(c5.os_incentive AS DECIMAL(34, 15)), 0) * COALESCE(CAST(fx.fx_rate AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS os_incentive_rmb
    FROM calc_5_hq_revenue_calculation c5
    LEFT JOIN fx_rate_raw fx
        ON c5.product_line_id = fx.product_line
        AND c5.period = fx.period
),

-- RMB計算1
calc_7_rmb_gp_calculation AS (
    SELECT
        *,
        CASE
            WHEN source = 'ACCRAW' THEN
                COALESCE(CAST(revenue_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(credit_note_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(local_cogs_amount_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(total_ship_fee_company_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(csc_amt_rmb AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS local_gp_rmb,
        CASE
            WHEN source = 'ACCRAW' THEN
                COALESCE(CAST(revenue_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(credit_note_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(cogs_amount_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(total_ship_fee_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(csc_amt_rmb AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS gp_pod_rmb
    FROM calc_6_rmb_conversion
),

-- RMB計算2
calc_8_rmb_hq_and_net_gp_calculation AS (
    SELECT
        *,
        CASE
            WHEN source = 'ACCRAW' THEN
                COALESCE(CAST(gp_pod_rmb AS DECIMAL(34, 15)), 0) - COALESCE(CAST(local_gp_rmb AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS hq_gp_rmb,
        CASE
            WHEN source = 'ACCRAW' THEN
                COALESCE(CAST(gp_pod_rmb AS DECIMAL(34, 15)), 0) + COALESCE(CAST(extra_rebate_rmb AS DECIMAL(34, 15)), 0) + COALESCE(CAST(nv_sellout_rebate_rmb AS DECIMAL(34, 15)), 0) + COALESCE(CAST(funding_rmb AS DECIMAL(34, 15)), 0) + COALESCE(CAST(os_incentive_rmb AS DECIMAL(34, 15)), 0)
            ELSE NULL
        END AS gp_net_rmb
    FROM calc_7_rmb_gp_calculation
),
accum_data AS (
    SELECT *
    FROM {{ source('ads_dev', 'csgp_accaci_weekly_accum') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads_dev', 'csgp_accaci_weekly_accum') }})
    AND period != (SELECT period FROM calc_1_base_gp_calculation LIMIT 1)
),
month_data_maxds AS (
    SELECT *
    FROM {{ source('ads_dev', 'csgp_accaci_monthly_accum') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads_dev', 'csgp_accaci_monthly_accum') }})
),
monthly_data AS (
    SELECT *
    FROM month_data_maxds
    WHERE period = (SELECT MAX(period) FROM month_data_maxds)
),
final_result AS (
    SELECT
        CAST(source AS VARCHAR) AS source,
        CAST(product_line_id AS VARCHAR) AS product_line_id,
        CAST(year AS VARCHAR) AS year,
        CAST(quarter AS VARCHAR) AS quarter,
        CAST(month AS VARCHAR) AS month,
        CAST(period AS VARCHAR) AS period,
        CAST(sold_to_customer AS VARCHAR) AS sold_to_customer,
        CAST(customer AS VARCHAR) AS customer,
        CAST(order_type AS VARCHAR) AS order_type,
        CAST(business_type AS VARCHAR) AS business_type,
        CAST(product_type AS VARCHAR) AS product_type,
        CAST(distribution AS VARCHAR) AS distribution,
        CAST(distribution_type AS VARCHAR) AS distribution_type,
        CAST(territory AS VARCHAR) AS territory,
        CAST(territory2 AS VARCHAR) AS territory2,
        CAST(branch AS VARCHAR) AS branch,
        CAST(branch2 AS VARCHAR) AS branch2,
        CAST(country_id AS VARCHAR) AS country_id,
        CAST(country_chinese AS VARCHAR) AS country_chinese,
        CAST(ou AS VARCHAR) AS ou,
        CAST(warranty AS VARCHAR) AS warranty,
        CAST(csid AS VARCHAR) AS csid,
        CAST(part_number AS VARCHAR) AS part_number,
        CAST(description AS VARCHAR) AS description,
        CAST(all_qty AS VARCHAR) AS all_qty,
        CAST(revenue_usd_hedge_rate AS VARCHAR) AS revenue_usd_hedge_rate,
        CAST(revenue_fxdiff AS VARCHAR) AS revenue_fxdiff,
        CAST(net_revenue_usd_agp AS VARCHAR) AS net_revenue_usd_agp,
        CAST(net_revenue_fxdiff AS VARCHAR) AS net_revenue_fxdiff,
        CAST(local_cogs_amount AS VARCHAR) AS local_cogs_amount,
        CAST(local_cogs_amount_fxdiff AS VARCHAR) AS local_cogs_amount_fxdiff,
        CAST(credit_note_hedge_rate AS VARCHAR) AS credit_note_hedge_rate,
        CAST(credit_note_fxdiff AS VARCHAR) AS credit_note_fxdiff,
        CAST(total_ship_fee_company AS VARCHAR) AS total_ship_fee_company,
        CAST(total_ship_fee_company_fxdiff AS VARCHAR) AS total_ship_fee_company_fxdiff,
        CAST(cost_period AS VARCHAR) AS cost_period,
        CAST(funding AS VARCHAR) AS funding,
        CAST(csc_ratio AS VARCHAR) AS csc_ratio,
        CAST(csc_amt AS VARCHAR) AS csc_amt,
        CAST(csc_amt_fxdiff AS VARCHAR) AS csc_amt_fxdiff,
        CAST(ship_fee_hq AS VARCHAR) AS ship_fee_hq,
        CAST(total_ship_fee_hq AS VARCHAR) AS total_ship_fee_hq,
        -- 處理計算項並轉為 VARCHAR
        CAST(total_ship_fee AS VARCHAR) AS total_ship_fee,
        CAST(total_ship_fee_fxdiff AS VARCHAR) AS total_ship_fee_fxdiff,
        CAST(hq_asc AS VARCHAR) AS hq_asc,
        CAST(hq_asc_fxdiff AS VARCHAR) AS hq_asc_fxdiff,
        CAST(cogs_amount AS VARCHAR) AS cogs_amount,
        CAST(cogs_amount_fxdiff AS VARCHAR) AS cogs_amount_fxdiff,
        CAST(opp_amount AS VARCHAR) AS opp_amount,
        CAST(extra_rebate AS VARCHAR) AS extra_rebate,
        CAST(nv_sellout_rebate AS VARCHAR) AS nv_sellout_rebate,
        CAST(os_incentive AS VARCHAR) AS os_incentive,
        CAST(local_gp AS VARCHAR) AS local_gp,
        CAST(local_gp_fxdiff AS VARCHAR) AS local_gp_fxdiff,
        CAST(hq_asp AS VARCHAR) AS hq_asp,
        CAST(hq_asp_fxdiff AS VARCHAR) AS hq_asp_fxdiff,
        CAST(hq_gp_unit AS VARCHAR) AS hq_gp_unit,
        CAST(hq_gp_unit_fxdiff AS VARCHAR) AS hq_gp_unit_fxdiff,
        CAST(hq_gp AS VARCHAR) AS hq_gp,
        CAST(hq_gp_usd_fxdiff AS VARCHAR) AS hq_gp_usd_fxdiff,
        CAST(hq_rev AS VARCHAR) AS hq_rev,
        CAST(hq_rev_usd_fxdiff AS VARCHAR) AS hq_rev_usd_fxdiff,
        CAST(gp_pod AS VARCHAR) AS gp_pod,
        CAST(gp_pod_fxdiff AS VARCHAR) AS gp_pod_fxdiff,
        CAST(gp_net AS VARCHAR) AS gp_net,
        CAST(gp_net_fxdiff AS VARCHAR) AS gp_net_fxdiff,
        CAST(model AS VARCHAR) AS model,
        CAST(segment AS VARCHAR) AS segment,
        CAST(gpu AS VARCHAR) AS gpu,
        CAST(cpu_vendor AS VARCHAR) AS cpu_vendor,
        CAST(cpu_platform AS VARCHAR) AS cpu_platform,
        CAST(cpu_sku AS VARCHAR) AS cpu_sku,
        CAST(lcd_code AS VARCHAR) AS lcd_code,
        CAST(cpu_n AS VARCHAR) AS cpu_n,
        CAST(acc_segment AS VARCHAR) AS acc_segment,
        CAST(revenue_rmb AS VARCHAR) AS revenue_rmb,
        CAST(net_revenue_rmb AS VARCHAR) AS net_revenue_rmb,
        CAST(local_cogs_amount_rmb AS VARCHAR) AS local_cogs_amount_rmb,
        CAST(credit_note_rmb AS VARCHAR) AS credit_note_rmb,
        CAST(total_ship_fee_rmb AS VARCHAR) AS total_ship_fee_rmb,
        CAST(cogs_amount_rmb AS VARCHAR) AS cogs_amount_rmb,
        CAST(opp_amount_rmb AS VARCHAR) AS opp_amount_rmb,
        CAST(extra_rebate_rmb AS VARCHAR) AS extra_rebate_rmb,
        CAST(nv_sellout_rebate_rmb AS VARCHAR) AS nv_sellout_rebate_rmb,
        CAST(funding_rmb AS VARCHAR) AS funding_rmb,
        CAST(os_incentive_rmb AS VARCHAR) AS os_incentive_rmb,
        CAST(total_ship_fee_company_rmb AS VARCHAR) AS total_ship_fee_company_rmb,
        CAST(csc_amt_rmb AS VARCHAR) AS csc_amt_rmb,
        CAST(hq_gp_rmb AS VARCHAR) AS hq_gp_rmb,
        CAST(gp_pod_rmb AS VARCHAR) AS gp_pod_rmb,
        CAST(gp_net_rmb AS VARCHAR) AS gp_net_rmb,
        CAST(local_gp_rmb AS VARCHAR) AS local_gp_rmb,
        CAST(is_cte AS VARCHAR) AS is_cte
    FROM calc_8_rmb_hq_and_net_gp_calculation
    UNION ALL
    SELECT source,
    product_line_id,
    year,
    quarter,
    month,
    period,
    sold_to_customer,
    customer,
    order_type,
    business_type,
    product_type,
    distribution,
    distribution_type,
    territory,
    territory2,
    branch,
    branch2,
    country_id,
    country_chinese,
    ou,
    warranty,
    csid,
    part_number,
    description,
    all_qty,
    revenue_usd_hedge_rate,
    revenue_fxdiff,
    net_revenue_usd_agp,
    net_revenue_fxdiff,
    local_cogs_amount,
    local_cogs_amount_fxdiff,
    credit_note_hedge_rate,
    credit_note_fxdiff,
    total_ship_fee_company,
    total_ship_fee_company_fxdiff,
    cost_period,
    funding,
    csc_ratio,
    csc_amt,
    csc_amt_fxdiff,
    ship_fee_hq,
    total_ship_fee_hq,
    total_ship_fee,
    total_ship_fee_fxdiff,
    hq_asc,
    hq_asc_fxdiff,
    cogs_amount,
    cogs_amount_fxdiff,
    opp_amount,
    extra_rebate,
    nv_sellout_rebate,
    os_incentive,
    local_gp,
    local_gp_fxdiff,
    hq_asp,
    hq_asp_fxdiff,
    hq_gp_unit,
    hq_gp_unit_fxdiff,
    hq_gp,
    hq_gp_usd_fxdiff,
    hq_rev,
    hq_rev_usd_fxdiff,
    gp_pod,
    gp_pod_fxdiff,
    gp_net,
    gp_net_fxdiff,
    model,
    segment,
    gpu,
    cpu_vendor,
    cpu_platform,
    cpu_sku,
    lcd_code,
    cpu_n,
    acc_segment,
    revenue_rmb,
    net_revenue_rmb,
    local_cogs_amount_rmb,
    credit_note_rmb,
    total_ship_fee_rmb,
    cogs_amount_rmb,
    opp_amount_rmb,
    extra_rebate_rmb,
    nv_sellout_rebate_rmb,
    funding_rmb,
    os_incentive_rmb,
    total_ship_fee_company_rmb,
    csc_amt_rmb,
    hq_gp_rmb,
    gp_pod_rmb,
    gp_net_rmb,
    local_gp_rmb,
    is_cte
    FROM accum_data
)
SELECT *
    ,{{get_current_time()}} AS dw_ins_time
    ,'{{var('ds')}}' AS ds
FROM final_result
WHERE period != (SELECT MAX(period) FROM monthly_data)
UNION ALL
SELECT source,
    product_line_id,
    year,
    quarter,
    month,
    period,
    sold_to_customer,
    customer,
    order_type,
    business_type,
    product_type,
    distribution,
    distribution_type,
    territory,
    territory2,
    branch,
    branch2,
    country_id,
    country_chinese,
    ou,
    warranty,
    csid,
    part_number,
    description,
    all_qty,
    revenue_usd_hedge_rate,
    revenue_fxdiff,
    net_revenue_usd_agp,
    net_revenue_fxdiff,
    local_cogs_amount,
    local_cogs_amount_fxdiff,
    credit_note_hedge_rate,
    credit_note_fxdiff,
    total_ship_fee_company,
    total_ship_fee_company_fxdiff,
    cost_period,
    funding,
    csc_ratio,
    csc_amt,
    csc_amt_fxdiff,
    ship_fee_hq,
    total_ship_fee_hq,
    total_ship_fee,
    total_ship_fee_fxdiff,
    hq_asc,
    hq_asc_fxdiff,
    cogs_amount,
    cogs_amount_fxdiff,
    opp_amount,
    extra_rebate,
    nv_sellout_rebate,
    os_incentive,
    local_gp,
    local_gp_fxdiff,
    hq_asp,
    hq_asp_fxdiff,
    hq_gp_unit,
    hq_gp_unit_fxdiff,
    hq_gp,
    hq_gp_usd_fxdiff,
    hq_rev,
    hq_rev_usd_fxdiff,
    gp_pod,
    gp_pod_fxdiff,
    gp_net,
    gp_net_fxdiff,
    model,
    segment,
    gpu,
    cpu_vendor,
    cpu_platform,
    cpu_sku,
    lcd_code,
    cpu_n,
    acc_segment,
    revenue_rmb,
    net_revenue_rmb,
    local_cogs_amount_rmb,
    credit_note_rmb,
    total_ship_fee_rmb,
    cogs_amount_rmb,
    opp_amount_rmb,
    extra_rebate_rmb,
    nv_sellout_rebate_rmb,
    funding_rmb,
    os_incentive_rmb,
    total_ship_fee_company_rmb,
    csc_amt_rmb,
    hq_gp_rmb,
    gp_pod_rmb,
    gp_net_rmb,
    local_gp_rmb,
    is_cte
    ,{{get_current_time()}} AS dw_ins_time
    ,'{{var('ds')}}' AS ds
FROM monthly_data