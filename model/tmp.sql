SELECT 
        ds
    FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_rmb') }}
    WHERE ds = (SELECT MAX(ds) FROM {{ source('ods', 's_apza002bid_csgp_exchange_rate_rmb') }})