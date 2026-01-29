{{ config(location=generate_external_location()) }}

with source as (
    select * from {{ source('bronze', 'stocks_daily') }}
),

renamed as (
    select
        ticker as asset_id,
        data_date,
        close as close_price,
        open as open_price,
        high as high_price,
        low as low_price,
        volume,
        vwap,
        case
            when ticker in ('AAPL', 'GOOGL', 'MSFT') then 'stock'
            when ticker = 'SPY' then 'index'
            else 'unknown'
        end as asset_type,
        case
            when ticker = 'AAPL' then 'Apple Inc.'
            when ticker = 'GOOGL' then 'Alphabet Inc.'
            when ticker = 'MSFT' then 'Microsoft Corporation'
            when ticker = 'SPY' then 'S&P 500 ETF'
            else ticker
        end as asset_name,
        __batch_id,
        __ingested_at
    from source
)

select * from renamed
where data_date is not null
  and close_price > 0
  and asset_id is not null
