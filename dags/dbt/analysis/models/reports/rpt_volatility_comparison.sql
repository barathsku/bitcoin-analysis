{{
  config(
    materialized='table'
  )
}}

with daily_returns as (
    select
        asset_id,
        asset_name,
        asset_type,
        daily_return
    from {{ ref('fct_daily_returns') }}
    where daily_return is not null
),

volatility_stats as (
    select
        asset_id,
        asset_name,
        asset_type,
        count(daily_return) as num_observations,
        avg(daily_return) as avg_daily_return,
        stddev(daily_return) as daily_volatility,
        -- Annualized volatility (sqrt(252) for trading days)
        stddev(daily_return) * sqrt(252) as annualized_volatility,
        min(daily_return) as min_daily_return,
        max(daily_return) as max_daily_return
    from daily_returns
    group by asset_id, asset_name, asset_type
),

ranked_volatility as (
    select
        asset_id,
        asset_name,
        asset_type,
        num_observations,
        round(avg_daily_return * 100, 4) as avg_daily_return_pct,
        round(daily_volatility * 100, 4) as daily_volatility_pct,
        round(annualized_volatility * 100, 2) as annualized_volatility_pct,
        round(min_daily_return * 100, 2) as min_daily_return_pct,
        round(max_daily_return * 100, 2) as max_daily_return_pct,
        rank() over (order by daily_volatility desc) as volatility_rank,
        case
            when asset_type = 'fiat' then 'Fiat Currencies'
            when asset_type = 'crypto' then 'Cryptocurrency'
            when asset_type = 'stock' then 'Stocks'
            when asset_type = 'index' then 'Market Index'
            else 'Other'
        end as category
    from volatility_stats
)

select
    asset_id,
    asset_name,
    asset_type,
    category,
    num_observations,
    avg_daily_return_pct,
    daily_volatility_pct,
    annualized_volatility_pct,
    min_daily_return_pct,
    max_daily_return_pct,
    volatility_rank
from ranked_volatility
order by volatility_rank
