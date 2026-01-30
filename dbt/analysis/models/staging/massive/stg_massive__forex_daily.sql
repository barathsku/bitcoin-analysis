{{ config(location=generate_external_location()) }}
-- Ideally this should be a view, but due to how DuckDB works with dbt, this needs to be exported to an external table format.

with source as (
    select * from {{ source('massive', 'forex_daily') }}
),

renamed as (
    select
        -- Extract currency pair from ticker (e.g., C:EURUSD --> EUR)
        case
            when ticker = 'C:EURUSD' then 'EUR'
            when ticker = 'C:GBPUSD' then 'GBP'
            else replace(replace(ticker, 'C:', ''), 'USD', '')
        end as asset_id,
        data_date,
        close as close_price,
        open as open_price,
        high as high_price,
        low as low_price,
        volume,
        'fiat' as asset_type,
        case
            when ticker = 'C:EURUSD' then 'Euro'
            when ticker = 'C:GBPUSD' then 'British Pound'
            else ticker
        end as asset_name,
        ticker as raw_ticker,
        __batch_id,
        __ingested_at
    from source
)

select * from renamed
where data_date is not null
  and close_price > 0
  and asset_id is not null
