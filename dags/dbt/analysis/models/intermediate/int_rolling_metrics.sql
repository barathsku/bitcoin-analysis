{{
  config(
    location='/home/bdn/coingecko-assessment/data/intermediate/int_rolling_metrics'
  )
}}

with returns as (
    select * from {{ ref('int_daily_returns') }}
),

rolling_calcs as (
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        daily_return,
        
        -- 7-day rolling metrics
        avg(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 6 preceding and current row
        ) as avg_return_7d,
        
        stddev(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 6 preceding and current row
        ) as volatility_7d,
        
        count(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 6 preceding and current row
        ) as obs_count_7d,
        
        -- 14-day rolling metrics
        avg(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 13 preceding and current row
        ) as avg_return_14d,
        
        stddev(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 13 preceding and current row
        ) as volatility_14d,
        
        count(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 13 preceding and current row
        ) as obs_count_14d,
        
        -- 30-day rolling metrics
        avg(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 29 preceding and current row
        ) as avg_return_30d,
        
        stddev(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 29 preceding and current row
        ) as volatility_30d,
        
        count(daily_return) over (
            partition by asset_id 
            order by data_date 
            rows between 29 preceding and current row
        ) as obs_count_30d,
        
        -- Cumulative return from start
        first_value(close_price) over (
            partition by asset_id 
            order by data_date 
            rows between unbounded preceding and unbounded following
        ) as first_price,
        
        (close_price / first_value(close_price) over (
            partition by asset_id 
            order by data_date 
            rows between unbounded preceding and current row
        ) - 1) as cumulative_return_from_start
        
    from returns
)

select * from rolling_calcs
