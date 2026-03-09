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
        'job': "csgp_bsgp_kpgap_accum",
        'alias': "PL, category, ag_no, period 及各個價差",
    },
    location = 's3://consumer-npspo/Tables/ads_dev/csgp_bsgp_kpgap_accum'    
)}}
WITH product_no_mapping AS (
    SELECT DISTINCT
        'NB_ODM' as project_no,
        product_no,
        buying_mode,
        group_no,
        vendor_code,
        asus_pn
    FROM {{ source('ods', 's_tpebiz01_scm_scm_eqo_nb_odm_prod_pn') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_tpebiz01_scm_scm_eqo_nb_odm_prod_pn') }})
    UNION
    SELECT DISTINCT
        'NB'as project_no,
        product_no,
        buying_mode,
        group_no,
        vendor_code,
        asus_pn
    FROM {{ source('ods', 's_tpebiz01_scm_scm_eqo_nb_prod_pn') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_tpebiz01_scm_scm_eqo_nb_prod_pn') }})
    UNION
    SELECT DISTINCT
        'NB_JDM'as project_no,
        product_no,
        buying_mode,
        group_no,
        vendor_code,
        asus_pn
    FROM {{ source('ods', 's_tpebiz01_scm_scm_eqo_nb_jdm_prod_pn') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_tpebiz01_scm_scm_eqo_nb_jdm_prod_pn') }})
), 
finkpdata_maxds AS (
    SELECT * 
    FROM {{ source('ods', 's_apza002bid_csgp_bsgp_finkpdata') }} 
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_bsgp_finkpdata') }})
),
finkpdata_maxgl_yearmonth AS (
    SELECT *
    FROM finkpdata_maxds
    WHERE gl_yearmonth = (SELECT MAX(gl_yearmonth) FROM finkpdata_maxds)
    --WHERE gl_yearmonth = '202510'
),
period_finkpdata AS (
    SELECT gl_yearmonth AS period
    FROM finkpdata_maxgl_yearmonth
    limit 1
),
calculated_range AS (
    SELECT 
        period AS first_ym,
        date_format(date_add('month', 1, date_parse(period, '%Y%m')), '%Y%m') as second_ym,
        date_format(date_add('month', 2, date_parse(period, '%Y%m')), '%Y%m') as third_ym
    FROM period_finkpdata
),
data_scm AS (
    SELECT scm.project_no,
    scm.group_no,
    product_no AS product_code,
    product_code_name,
    is_odm,
    scm.vendor_code,
    vendor_site,
    ems_org_code,
    scm.buying_mode,
    scm.asus_pn,
    asus_pn_desc,
    pn_group_no,
    po_price,
    po_effective_date,
    po_expired_date,
    real_pn_group_no,
    ag_no,
    dw_ins_time,
    ds 
    FROM (SELECT * FROM {{ source('ads', 'ads_dl_scm_ems_equo_scm_eqo_all_pn_price_vw_dl') }}) scm
    LEFT JOIN
    product_no_mapping
    ON UPPER(scm.project_no) = UPPER(product_no_mapping.project_no)
    AND UPPER(scm.buying_mode) = UPPER(product_no_mapping.buying_mode)
    AND UPPER(scm.group_no) = UPPER(product_no_mapping.group_no)
    AND UPPER(scm.vendor_code) = UPPER(product_no_mapping.vendor_code)
    AND UPPER(scm.asus_pn) = UPPER(product_no_mapping.asus_pn)
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ads', 'ads_dl_scm_ems_equo_scm_eqo_all_pn_price_vw_dl') }})
    AND scm.project_no IN ('NB', 'NB_JDM', 'NB_ODM')
    AND is_odm IN ('N', 'Y', 'J')
    AND scm.buying_mode = 'B'
    AND scm.group_no IN (
      (SELECT first_ym FROM calculated_range),
      (SELECT second_ym FROM calculated_range),
      (SELECT third_ym FROM calculated_range)
  )
),
bsgp_itemcode_tmp AS (
    SELECT * FROM {{ source('ods', 's_apza002bid_csgp_bsgp_itemcode') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_bsgp_itemcode') }})
),
scm_with_category AS (
    SELECT
        T1.product_code AS product_line,
        CASE
            WHEN T1.asus_pn_desc LIKE '%BRA%' THEN 'BR'
            WHEN T2.category IS NOT NULL THEN T2.category
            WHEN T1.asus_pn_desc LIKE 'DDR%'
            OR T1.asus_pn_desc LIKE 'LPD%'
            OR T1.asus_pn_desc LIKE 'GDDR%'
            OR T1.asus_pn_desc LIKE 'LP%'
            THEN 'DDR'
            ELSE 'SSD'
        END AS category,
        T1.ag_no,
        T1.po_price,
        T1.group_no AS period
    FROM
        data_scm AS T1
    LEFT JOIN
        bsgp_itemcode_tmp AS T2
        ON SUBSTR(T1.asus_pn, 1, 2) = T2.item_code
),
scm_avg_equo AS (
    SELECT product_line,
        category,
        ag_no,
        avg(CAST(po_price AS decimal(38, 15))) AS equo_price,
        period
    FROM scm_with_category
    WHERE category IN ('BATT', 'SSD', 'LCD', 'HDD', 'DDR', 'Flash')
    GROUP BY product_line, category, ag_no, period
),
report_with_fx_rate AS (
    SELECT product_line,
            category,
            ag_no,
            try_cast(net_sale_amount as decimal(38, 15)) AS net_sale_amount,
            try_cast(material_cost as decimal(38, 15)) AS material_cost,
            try_cast(quantity as decimal(38, 15)) AS quantity,
            CAST(fx_rate AS decimal(38, 15)) AS fx_rate
    FROM finkpdata_maxgl_yearmonth finkpdata
    LEFT JOIN (
        SELECT distinct *
        FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_ntd') }} 
        WHERE CAST(period AS varchar) = (SELECT first_ym FROM calculated_range)
        AND currency = 'USD'
        AND ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_ntd') }})
    ) ntd_fx_rate
    ON CAST(ntd_fx_rate.period AS VARCHAR) = finkpdata.gl_yearmonth
),
report_data AS 
(
    SELECT product_line,
            category,
            ag_no,
            (sum(net_sale_amount) / nullif(sum(quantity), 0)) / nullif(fx_rate, 0) AS equo_price,
            (sum(material_cost) / nullif(sum(quantity), 0)) / nullif(fx_rate, 0) AS cogs_price
    FROM report_with_fx_rate
    GROUP BY product_line, category, ag_no, fx_rate
),
final_data AS 
(
    SELECT COALESCE(scm.product_line, report.product_line) AS product_line,
        COALESCE(scm.category, report.category) AS category,
        COALESCE(scm.ag_no, report.ag_no) AS ag_no,
        CASE 
            WHEN scm.period = (SELECT first_ym FROM calculated_range) 
            THEN report.equo_price
            ELSE scm.equo_price 
        END AS equo_price,
        report.cogs_price AS cogs_price,
        CASE 
            WHEN scm.equo_price IS NOT NULL AND report.cogs_price IS NOT NULL 
            THEN report.cogs_price - scm.equo_price
            ELSE NULL 
        END AS bs_gap,
        scm.period AS period
    FROM scm_avg_equo AS scm
    FULL OUTER JOIN report_data AS report
    ON scm.product_line = report.product_line
    AND scm.category = report.category
    AND scm.ag_no = report.ag_no
),
kpgap_accum AS
(
    SELECT product_line,
            category,
            ag_no,
            period,
            equo_price,
            cogs_price,
            bs_gap
        FROM {{ this }}
        WHERE ds = (SELECT MAX(ds) FROM {{ this }})
),
kpgap_accum_max AS
(
    SELECT product_line,
            category,
            ag_no,
            period,
            equo_price,
            cogs_price,
            bs_gap
        FROM kpgap_accum
        WHERE period = (SELECT MAX(period) FROM kpgap_accum)
),
target_periods AS (
    SELECT DISTINCT period 
    FROM final_data
    UNION
    SELECT DISTINCT period
    FROM kpgap_accum
),
all_products AS (
    SELECT distinct product_line, category, ag_no 
    FROM final_data
    UNION
    SELECT distinct product_line, category, ag_no 
    FROM kpgap_accum
),
skeleton AS (
    SELECT 
        p.product_line, 
        p.category, 
        p.ag_no, 
        t.period
    FROM all_products p
    CROSS JOIN target_periods t
    WHERE period is not null
)
SELECT 
    s.product_line,
    s.category,
    s.ag_no,
    s.period,
    CAST(COALESCE(fd.equo_price, ka.equo_price, ka_max.equo_price) AS decimal(38, 15)) AS equo_price,
    CAST(COALESCE(fd.cogs_price, ka.cogs_price, ka_max.cogs_price) AS decimal(38, 15)) AS cogs_price,
    (COALESCE(fd.cogs_price, ka.cogs_price, ka_max.cogs_price) - COALESCE(fd.equo_price, ka.equo_price, ka_max.equo_price)) AS bs_gap,
    {{get_current_time()}} AS dw_ins_time,
    '{{var('ds')}}' AS ds
FROM skeleton s
LEFT JOIN final_data AS fd
ON s.product_line = fd.product_line
AND s.category = fd.category
AND s.ag_no = fd.ag_no
AND s.period = fd.period
LEFT JOIN kpgap_accum AS ka
ON s.product_line = ka.product_line
AND s.category = ka.category
AND s.ag_no = ka.ag_no
AND s.period = ka.period
LEFT JOIN kpgap_accum_max AS ka_max
ON s.product_line = ka_max.product_line
AND s.category = ka_max.category
AND s.ag_no = ka_max.ag_no;