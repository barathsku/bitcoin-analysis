{#
  Throws error if there are unexpected data gaps for critical assets.
  
  - BTC (crypto) should have no gaps (trades 24/7)
  - Stocks should have no gaps on trading days
  - Forex gaps on weekends are acceptable, hence excluded
  
  This test will throw an error if any rows are returned.
#}

{{ config(severity='error') }}

select
    asset_id,
    asset_type,
    missing_date,
    is_trading_day
from {{ ref('int_market__data_gaps') }}
where 
    -- BTC should have no gaps at all
    asset_type = 'crypto'
    -- Stocks should have no gaps on trading days (Mon-Fri) excluding holidays
    or (asset_type in ('stock', 'index') and is_trading_day = 1)
order by asset_id, missing_date
limit 100  -- Show first 100 gaps if test fails
