{% macro get_current_time(format='%Y-%m-%d %H:%i:%s') %}
date_format(AT_TIMEZONE(CURRENT_TIMESTAMP, 'Asia/Taipei'), '{{format}}' )
{% endmacro %}


{% macro gen_1179_hq_condition(configs) %}

    {# 
       [更新] 移除 extra_cols 參數
       現在邏輯改為：只要 has_source 為 True (代表是 1179 表)，
       就自動計算 ship_fee_hq, cogs_amount, cogs_amount_fxdiff 的平均。
    #}

    {% for config in configs %}
        
        {% set input_cte = config.input_cte %}
        {% set prefix = config.prefix %}
        {% set suffix = config.suffix | default('') %}
        {% set has_source = config.has_source | default(false) %}

        {# 1. Source 欄位處理 #}
        {% set source_col_select = ', source' if has_source else '' %}
        {% set source_col_partition = ', source' if has_source else '' %}

        {# 2. 1179 專屬固定欄位處理 #}
        {% set fixed_cols_agg = '' %}
        {% set fixed_cols_select = '' %}

        {% if has_source %}
            {# 只有 1179 (有 source 的表) 才需要算這三個 #}
            {% set fixed_cols_agg %}
                , AVG(ship_fee_hq) AS ship_fee_hq
                , SUM(cogs_amount)/SUM(all_qty) AS cogs_amount
                , SUM(cogs_amount_fxdiff)/SUM(all_qty) AS cogs_amount_fxdiff
            {% endset %}

            {% set fixed_cols_select %}
                , ship_fee_hq
                , cogs_amount
                , cogs_amount_fxdiff
            {% endset %}
        {% endif %}
        
        {# 定義 CTE 名稱 #}
        {% set cte_pn_period = prefix ~ '_pn_period' ~ suffix %}
        {% set cte_pn_max = prefix ~ '_pn_maxperiod' ~ suffix %}
        {% set cte_model_period = prefix ~ '_model_cpu_sku_period' ~ suffix %}
        {% set cte_model_max = prefix ~ '_model_cpu_sku_maxperiod' ~ suffix %}

        {# 3. 邏輯開始 #}

        {# PN Period #}
        {{ cte_pn_period }} AS (
            SELECT 
                part_number, 
                period
                {{ source_col_select }}
                , avg(item_cost_usd) AS item_cost_usd
                {{ fixed_cols_agg }}  {# 1179 會自動插入 AVG #}
            FROM {{ input_cte }}
            GROUP BY part_number, period {{ source_col_select }}
        ),

        {# PN Max Period #}
        {{ cte_pn_max }} AS (
            SELECT 
                part_number, 
                period 
                {{ source_col_select }}
                , item_cost_usd
                {{ fixed_cols_select }} {# 1179 會自動透傳欄位 #}
            FROM (
                SELECT 
                    part_number, 
                    period
                    {{ source_col_select }}
                    , item_cost_usd
                    {{ fixed_cols_select }}
                    , ROW_NUMBER() OVER (
                        PARTITION BY part_number {{ source_col_partition }}
                        ORDER BY period DESC
                    ) as rn
                FROM {{ cte_pn_period }}
                WHERE period <= (SELECT period FROM accaci_raw limit 1)
            ) sub
            WHERE rn = 1
        ),

        {# Model CPU SKU Period #}
        {{ cte_model_period }} AS (
            SELECT 
                model, 
                cpu_sku, 
                period
                {{ source_col_select }}
                , avg(item_cost_usd) AS item_cost_usd
                {{ fixed_cols_agg }}  {# 1179 會自動插入 AVG #}
            FROM {{ input_cte }}
            GROUP BY model, cpu_sku, period {{ source_col_select }}
        ),

        {# Model Max Period #}
        {{ cte_model_max }} AS (
            SELECT 
                model, 
                cpu_sku, 
                period 
                {{ source_col_select }}
                , item_cost_usd
                {{ fixed_cols_select }} {# 1179 會自動透傳欄位 #}
            FROM (
                SELECT 
                    model, 
                    cpu_sku, 
                    period
                    {{ source_col_select }}
                    , item_cost_usd
                    {{ fixed_cols_select }}
                    , ROW_NUMBER() OVER (
                        PARTITION BY model, cpu_sku {{ source_col_partition }}
                        ORDER BY period DESC
                    ) as rn
                FROM {{ cte_model_period }}
                WHERE period <= (SELECT period FROM accaci_raw limit 1)
            ) sub
            WHERE rn = 1
        )
        
        {# 處理迴圈逗號 #}
        {%- if not loop.last -%}
            ,
        {%- endif -%}

    {% endfor %}

{% endmacro %}

{% macro gen_aci_condition(configs) %}

    {# 
       configs 結構預期:
       [
         { 'input_cte': 'data_aci_item_cost_raw', 'prefix': 'data_aci_item_cost', 'suffix': '' },
         ...
       ]
    #}

    {% for config in configs %}
        
        {% set input_cte = config.input_cte %}
        {% set prefix = config.prefix %}
        {% set suffix = config.suffix | default('') %}
        
        {# 定義輸出 CTE 名稱 (只產出這一個) #}
        {% set cte_pn_period = prefix ~ '_pn_period' ~ suffix %}

        {# 
           邏輯: 直接輸出，不需 Max Period 運算
           (假設 input_cte 已經改好欄位名稱：part_number, period, item_cost_usd)
        #}
        {{ cte_pn_period }} AS (
            SELECT 
                part_number, 
                period, 
                item_cost_usd
            FROM {{ input_cte }}
        )
        
        {# 處理迴圈逗號 #}
        {%- if not loop.last -%}
            ,
        {%- endif -%}

    {% endfor %}

{% endmacro %}

{% macro gen_hq_model_cpu_branch_condition(configs) %}

    {# 
       configs 結構預期:
       [
         {
           'input_cte': 'data_hq_branch_raw',
           'prefix':    'data_hq_model_cpu_branch',
           'suffix':    ''
         },
         ...
       ]
    #}

    {% for config in configs %}
        
        {% set input_cte = config.input_cte %}
        {% set prefix = config.prefix %}
        {% set suffix = config.suffix | default('') %}
        
        {# 定義輸出 CTE 名稱 #}
        {% set cte_branch = prefix ~ suffix %}

        {# 
           邏輯修改：
           1. 加入 GROUP BY (sales_team, model, cpu_sku, period)
           2. 加入 AVG(item_cost_usd)
           3. 維持原本的 ROW_NUMBER 邏輯來取最新一筆
        #}
        {{ cte_branch }} AS (
            SELECT 
                sales_team,
                model,
                cpu_sku,
                period,
                item_cost_usd
            FROM (
                SELECT 
                    sales_team,
                    model,
                    cpu_sku,
                    period,
                    AVG(item_cost_usd) as item_cost_usd, 
                    ROW_NUMBER() OVER (
                        PARTITION BY sales_team, model, cpu_sku 
                        ORDER BY period DESC
                    ) as rn
                FROM {{ input_cte }}
                WHERE period <= (SELECT period FROM accaci_raw limit 1)
                GROUP BY sales_team, model, cpu_sku, period
            ) sub
            WHERE rn = 1
        )
        
        {# 處理迴圈逗號 #}
        {%- if not loop.last -%}
            ,
        {%- endif -%}

    {% endfor %}

{% endmacro %}