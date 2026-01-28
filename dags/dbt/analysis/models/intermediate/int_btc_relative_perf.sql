{{
  config(
    location='/home/bdn/coingecko-assessment/data/intermediate/int_btc_relative_perf'
  )
}}

with asset_returns as (
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        daily_return
    from {{ ref('int_daily_returns') }}
),

btc_returns as (
    select
        data_date,
        daily_return as btc_daily_return
    from asset_returns
    where asset_id = 'BTC'
),

relative_performance as (
    select
        a.asset_id,
        a.asset_name,
        a.asset_type,
        a.data_date,
        a.daily_return as asset_return,
        b.btc_daily_return,
        -- Relative performance vs Bitcoin (percentage point difference)
        coalesce(a.daily_return, 0) - coalesce(b.btc_daily_return, 0) as return_vs_btc,
        -- Outperformance flag
        case
            when a.daily_return > b.btc_daily_return then 1
            else 0
        end as outperformed_btc_flag
    from asset_returns a
    left join btc_returns b
        on a.data_date = b.data_date
)

select * from relative_performance
where asset_id != 'BTC'  -- Exclude BTC vs itself
