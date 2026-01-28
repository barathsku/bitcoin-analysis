{{ config(location=generate_external_location()) }}

select distinct
    asset_id,
    asset_name,
    asset_type
from {{ ref('stg_unified_prices') }}
order by
    case asset_type
        when 'crypto' then 1
        when 'fiat' then 2
        when 'stock' then 3
        when 'index' then 4
        else 5
    end,
    asset_id
