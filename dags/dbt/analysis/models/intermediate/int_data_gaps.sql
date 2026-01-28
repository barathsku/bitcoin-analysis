{{
  config(
    location='/home/bdn/coingecko-assessment/data/intermediate/int_data_gaps'
  )
}}

{#
  Detects missing dates per asset by comparing actual data dates
  against expected trading days (or all days for crypto/fiat).
  
  This model makes data quality issues explicit and queryable.
#}

with expected_dates as (
    -- For each asset, determine which dates we expect data for
    select
        a.asset_id,
        a.asset_type,
        d.date as expected_date,
        d.is_trading_day
    from {{ ref('dim_assets') }} a
    cross join {{ ref('dim_dates') }} d
    -- Filter: Stocks/index only expect trading days; crypto/fiat expect all days
    where (a.asset_type in ('crypto', 'fiat') or d.is_trading_day = 1)
),

actual_dates as (
    select distinct
        asset_id,
        data_date
    from {{ ref('stg_unified_prices') }}
),

gaps as (
    select
        e.asset_id,
        e.asset_type,
        e.expected_date as missing_date,
        e.is_trading_day
    from expected_dates e
    left join actual_dates a
        on e.asset_id = a.asset_id
        and e.expected_date = a.data_date
    where a.data_date is null
      -- Only flag gaps within the data range (not before first observation)
      and e.expected_date >= (
          select min(data_date) from actual_dates where asset_id = e.asset_id
      )
      and e.expected_date <= (
          select max(data_date) from actual_dates where asset_id = e.asset_id
      )
)

select
    asset_id,
    asset_type,
    missing_date,
    is_trading_day,
    current_timestamp as detected_at
from gaps
order by asset_id, missing_date
