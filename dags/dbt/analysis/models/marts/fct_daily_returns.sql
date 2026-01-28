{{ config(location=generate_external_location()) }}

select
    asset_id,
    asset_name,
    asset_type,
    data_date,
    close_price,
    daily_return
from {{ ref('int_daily_returns') }}
order by asset_id, data_date
