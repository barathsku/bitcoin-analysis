{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('bronze', 'market_chart') }}
),

renamed as (
    select
        data_date,
        data_timestamp,
        price_usd as close_price,
        market_cap_usd,
        volume_24h_usd as volume_usd,
        'BTC' as asset_id,
        'Bitcoin' as asset_name,
        'crypto' as asset_type,
        __batch_id,
        __ingested_at
    from source
),

-- dedup to account for multiple CoinGecko records per day; we take the latest data_timestamp value
deduplicated as (
    select *,
        row_number() over (partition by data_date order by data_timestamp desc, __ingested_at desc) as rn
    from renamed
)

select * exclude(rn) from deduplicated
where data_date is not null
  and close_price > 0
  and rn = 1
