{#
  Singular test: Fails if there are unexpected data gaps for critical assets.
  
  - BTC (crypto) should have no gaps (trades 24/7)
  - Stocks should have no gaps on trading days
  - Forex gaps on weekends are acceptable, hence excluded
  
  This test will fail the dbt run/build if any rows are returned.
#}

select
    asset_id,
    asset_type,
    missing_date,
    is_trading_day
from {{ ref('int_data_gaps') }}
where 
    -- BTC should have no gaps at all
    asset_type = 'crypto'
    -- Stocks should have no gaps on trading days (Mon-Fri)
    or (asset_type in ('stock', 'index') and is_trading_day = 1)
order by asset_id, missing_date
limit 100  -- Show first 100 gaps if test fails
