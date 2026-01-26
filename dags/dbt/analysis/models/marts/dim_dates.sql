{{
  config(
    materialized='table'
  )
}}

with all_dates as (
    select distinct data_date
    from {{ ref('stg_unified_prices') }}
),

date_attributes as (
    select
        data_date,
        extract(year from data_date) as year,
        extract(month from data_date) as month,
        extract(day from data_date) as day,
        extract(dayofweek from data_date) as day_of_week,
        extract(quarter from data_date) as quarter,
        extract(dayofyear from data_date) as day_of_year,
        -- Is trading day? (Mon-Fri)
        case
            when extract(dayofweek from data_date) in (1, 7) then 0  -- Sunday=1, Saturday=7
            else 1
        end as is_trading_day,
        -- Year-to-date
        case
            when extract(month from data_date) = 1 and extract(day from data_date) = 1
            then 1
            else 0
        end as is_year_start
    from all_dates
)

select
    data_date as date,
    year,
    month,
    day,
    day_of_week,
    quarter,
    day_of_year,
    is_trading_day,
    is_year_start,
    strftime(data_date, '%Y-%m') as year_month,
    strftime(data_date, '%Y-Q%q') as year_quarter
from date_attributes
order by data_date
