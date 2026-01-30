with forex as (
    select * from {{ ref('stg_massive__forex_daily') }}
),
holidays as (
    select * from {{ ref('stg_static__market_holidays') }}
)
select
    f.asset_id,
    f.data_date,
    h.holiday_name,
    h.market_code,
    f.close_price
from forex f
join holidays h on f.data_date = h.holiday_date
where
    (f.asset_id = 'GBP' and h.market_code = 'UK')
    or
    (f.asset_id = 'EUR' and h.market_code = 'EU')
order by f.data_date
