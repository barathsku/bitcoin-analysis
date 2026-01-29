{{ config(location=generate_external_location()) }}

select
    asset_id,
    asset_name,
    asset_type,
    data_date,
    close_price,
    daily_return,
    avg_return_7d,
    volatility_7d,
    obs_count_7d,
    avg_return_14d,
    volatility_14d,
    obs_count_14d,
    avg_return_30d,
    volatility_30d,
    obs_count_30d,
    cumulative_return_from_start
from {{ ref('int_market__rolling_metrics') }}
order by asset_id, data_date
