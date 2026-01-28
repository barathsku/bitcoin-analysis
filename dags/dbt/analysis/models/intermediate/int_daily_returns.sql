{{ config(location=generate_external_location()) }}

with price_data as (
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        lag(close_price) over (partition by asset_id order by data_date) as prev_close_price,
        lag(data_date) over (partition by asset_id order by data_date) as prev_date
    from {{ ref('stg_unified_prices') }}
),

returns_calc as (
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        prev_close_price,
        -- Calculate daily return
        case
            when prev_close_price is not null and prev_close_price > 0
            then (close_price - prev_close_price) / prev_close_price
            else null
        end as daily_return,
        -- Days since previous observation
        case
            when prev_date is not null
            then datediff('day', prev_date, data_date)
            else null
        end as days_gap
    from price_data
)

select
    asset_id,
    asset_name,
    asset_type,
    data_date,
    close_price,
    prev_close_price,
    daily_return,
    days_gap
from returns_calc
where data_date is not null
