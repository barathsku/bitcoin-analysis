{{ config(location=generate_external_location()) }}

select
    asset_id,
    asset_name,
    asset_type,
    data_date,
    close_price,
    daily_return,
    __batch_id,
    __ingested_at
from {{ ref('int_market__daily_returns') }}
order by asset_id, data_date
