{{ config(location=generate_external_location()) }}

with daily_returns as (
    select
        data_date,
        asset_id,
        asset_name,
        daily_return
    from {{ ref('fct_daily_returns') }}
    where daily_return is not null
),

correlations as (
    select
        a.asset_id as asset_a,
        a.asset_name as asset_a_name,
        b.asset_id as asset_b,
        b.asset_name as asset_b_name,
        corr(a.daily_return, b.daily_return) as correlation,
        count(*) as observation_count
    from daily_returns a
    join daily_returns b on a.data_date = b.data_date
    group by 1, 2, 3, 4
)

select * from correlations
where correlation is not null
order by asset_a, correlation desc
