{{ config(location=generate_external_location()) }}

with latest_date as (
    select max(data_date) as max_date
    from {{ ref('fct_daily_prices') }}
),

btc_prices as (
    select
        data_date,
        close_price as btc_price
    from {{ ref('fct_daily_prices') }}
    where asset_id = 'BTC'
),

year_ago_date as (
    select
        max_date,
        max_date - INTERVAL '365 days' as one_year_ago
    from latest_date
),

-- Scenario 1: Lump sum $1,000 invested one year ago
lump_sum_btc as (
    select
        'BTC' as asset,
        'Lump Sum' as strategy,
        1000.0 as initial_investment,
        bp_start.btc_price as purchase_price,
        bp_end.btc_price as current_price,
        1000.0 / bp_start.btc_price as btc_purchased,
        (1000.0 / bp_start.btc_price) * bp_end.btc_price as current_value,
        ((1000.0 / bp_start.btc_price) * bp_end.btc_price) - 1000.0 as profit_loss,
        (((1000.0 / bp_start.btc_price) * bp_end.btc_price) / 1000.0 - 1) as return_pct
    from year_ago_date yad
    left join btc_prices bp_start
        on bp_start.data_date >= yad.one_year_ago
    left join btc_prices bp_end
        on bp_end.data_date = yad.max_date
    where bp_start.data_date is not null
    order by bp_start.data_date
    limit 1
),

lump_sum_usd as (
    select
        'USD' as asset,
        'Lump Sum' as strategy,
        1000.0 as initial_investment,
        1.0 as purchase_price,
        1.0 as current_price,
        1000.0 as btc_purchased,  -- Not BTC but USD held
        1000.0 as current_value,
        0.0 as profit_loss,
        0.0 as return_pct
),

-- Scenario 2: DCA $100/month for 12 months
monthly_dates as (
    select
        yad.max_date,
        CASE seq.month_offset
            WHEN 0 THEN yad.max_date
            WHEN 1 THEN yad.max_date - INTERVAL '1 month'
            WHEN 2 THEN yad.max_date - INTERVAL '2 months'
            WHEN 3 THEN yad.max_date - INTERVAL '3 months'
            WHEN 4 THEN yad.max_date - INTERVAL '4 months'
            WHEN 5 THEN yad.max_date - INTERVAL '5 months'
            WHEN 6 THEN yad.max_date - INTERVAL '6 months'
            WHEN 7 THEN yad.max_date - INTERVAL '7 months'
            WHEN 8 THEN yad.max_date - INTERVAL '8 months'
            WHEN 9 THEN yad.max_date - INTERVAL '9 months'
            WHEN 10 THEN yad.max_date - INTERVAL '10 months'
            WHEN 11 THEN yad.max_date - INTERVAL '11 months'
        END as purchase_date,
        seq.month_offset
    from year_ago_date yad
    cross join (
        select 0 as month_offset union all select 1 union all select 2 union all
        select 3 union all select 4 union all select 5 union all select 6 union all
        select 7 union all select 8 union all select 9 union all select 10 union all select 11
    ) seq
),

dca_purchases as (
    select
        md.month_offset,
        md.purchase_date,
        bp.btc_price as purchase_price,
        100.0 / bp.btc_price as btc_purchased
    from monthly_dates md
    left join btc_prices bp
        on bp.data_date >= md.purchase_date
    where bp.data_date is not null
    qualify row_number() over (partition by md.month_offset order by bp.data_date) = 1
),

dca_summary as (
    select
        'BTC' as asset,
        'DCA' as strategy,
        sum(100.0) as initial_investment,
        avg(purchase_price) as purchase_price,  -- Average price
        (select btc_price from btc_prices where data_date = (select max_date from latest_date)) as current_price,
        sum(btc_purchased) as btc_purchased,
        sum(btc_purchased) * (select btc_price from btc_prices where data_date = (select max_date from latest_date)) as current_value,
        (sum(btc_purchased) * (select btc_price from btc_prices where data_date = (select max_date from latest_date))) - sum(100.0) as profit_loss,
        ((sum(btc_purchased) * (select btc_price from btc_prices where data_date = (select max_date from latest_date))) / sum(100.0) - 1) as return_pct
    from dca_purchases
),

combined_results as (
    select * from lump_sum_btc
    union all
    select * from lump_sum_usd
    union all
    select * from dca_summary
)

select
    asset,
    strategy,
    round(initial_investment, 2) as initial_investment_usd,
    round(purchase_price, 2) as avg_purchase_price,
    round(current_price, 2) as current_price,
    round(btc_purchased, 8) as btc_amount,
    round(current_value, 2) as current_value_usd,
    round(profit_loss, 2) as profit_loss_usd,
    round(return_pct * 100, 2) as return_pct
from combined_results
order by
    case asset when 'USD' then 1 when 'BTC' then 2 end,
    case strategy when 'Lump Sum' then 1 when 'DCA' then 2 end
