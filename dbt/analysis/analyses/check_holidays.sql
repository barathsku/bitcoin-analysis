select
    holiday_date,
    holiday_name,
    market_code,
    dayname(cast(holiday_date as date)) as day_of_week
from {{ ref('stg_static__market_holidays') }}
where market_status = 'closed'
and dayofweek(cast(holiday_date as date)) not in (0, 6) -- Exclude weekends (Sun=0, Sat=6)
order by holiday_date
