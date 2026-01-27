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
        price_usd as close_price,
        market_cap_usd,
        volume_24h_usd as volume_usd,
        'BTC' as asset_id,
        'Bitcoin' as asset_name,
        'crypto' as asset_type,
        __batch_id,
        __ingested_at
    from source
)

select * from renamed
where data_date is not null
  and close_price > 0
