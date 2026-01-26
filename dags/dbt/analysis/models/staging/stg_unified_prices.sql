{{
  config(
    materialized='view'
  )
}}

-- Add USD as the cash baseline (constant at 1.0)
with usd_baseline as (
    select distinct
        'USD' as asset_id,
        'US Dollar' as asset_name,
        'fiat' as asset_type,
        data_date,
        1.0 as close_price,
        1.0 as open_price,
        1.0 as high_price,
        1.0 as low_price,
        0.0 as volume,
        null as vwap,
        null as market_cap_usd,
        null as volume_usd,
        'synthetic' as __batch_id,
        current_timestamp as __ingested_at
    from {{ ref('stg_btc_prices') }}  -- Use BTC dates as the master calendar
),

-- Union all price data
unified as (
    -- BTC prices
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        null as open_price,
        null as high_price,
        null as low_price,
        null as volume,
        null as vwap,
        market_cap_usd,
        volume_usd,
        __batch_id,
        __ingested_at
    from {{ ref('stg_btc_prices') }}

    union all

    -- Stock prices
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        open_price,
        high_price,
        low_price,
        volume,
        vwap,
        null as market_cap_usd,
        null as volume_usd,
        __batch_id,
        __ingested_at
    from {{ ref('stg_stock_prices') }}

    union all

    -- Forex rates
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        open_price,
        high_price,
        low_price,
        volume,
        null as vwap,
        null as market_cap_usd,
        null as volume_usd,
        __batch_id,
        __ingested_at
    from {{ ref('stg_forex_rates') }}

    union all

    -- USD baseline
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        open_price,
        high_price,
        low_price,
        volume,
        vwap,
        market_cap_usd,
        volume_usd,
        __batch_id,
        __ingested_at
    from usd_baseline
)

select * from unified
