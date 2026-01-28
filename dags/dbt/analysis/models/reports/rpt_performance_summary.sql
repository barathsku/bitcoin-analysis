{{ config(location=generate_external_location()) }}

with latest_date as (
    select max(data_date) as max_date
    from {{ ref('fct_daily_prices') }}
),

price_windows as (
    select
        p.asset_id,
        p.asset_name,
        p.asset_type,
        p.data_date,
        p.close_price,
        ld.max_date,
        -- Calculate lookback dates
        ld.max_date - INTERVAL '7 days' as date_7d_ago,
        ld.max_date - INTERVAL '30 days' as date_1m_ago,
        ld.max_date - INTERVAL '90 days' as date_3m_ago,
        ld.max_date - INTERVAL '180 days' as date_6m_ago,
        ld.max_date - INTERVAL '365 days' as date_1y_ago,
        date_trunc('year', ld.max_date) as ytd_start
    from {{ ref('fct_daily_prices') }} p
    cross join latest_date ld
),

current_prices as (
    select
        asset_id,
        asset_name,
        asset_type,
        close_price as current_price
    from price_windows
    where data_date = max_date
),

-- Get prices at each lookback period (nearest date)
lookback_prices as (
    select
        asset_id,
        '7D' as period,
        (select close_price from price_windows pw
         where pw.asset_id = p.asset_id
         and pw.data_date >= p.date_7d_ago
         order by pw.data_date limit 1) as start_price
    from price_windows p
    where p.data_date = p.max_date
    
    union all
    
    select
        asset_id,
        '1M' as period,
        (select close_price from price_windows pw
         where pw.asset_id = p.asset_id
         and pw.data_date >= p.date_1m_ago
         order by pw.data_date limit 1) as start_price
    from price_windows p
    where p.data_date = p.max_date
    
    union all
    
    select
        asset_id,
        '3M' as period,
        (select close_price from price_windows pw
         where pw.asset_id = p.asset_id
         and pw.data_date >= p.date_3m_ago
         order by pw.data_date limit 1) as start_price
    from price_windows p
    where p.data_date = p.max_date
    
    union all
    
    select
        asset_id,
        '6M' as period,
        (select close_price from price_windows pw
         where pw.asset_id = p.asset_id
         and pw.data_date >= p.date_6m_ago
         order by pw.data_date limit 1) as start_price
    from price_windows p
    where p.data_date = p.max_date
    
    union all
    
    select
        asset_id,
        '1Y' as period,
        (select close_price from price_windows pw
         where pw.asset_id = p.asset_id
         and pw.data_date >= p.date_1y_ago
         order by pw.data_date limit 1) as start_price
    from price_windows p
    where p.data_date = p.max_date
    
    union all
    
    select
        asset_id,
        'YTD' as period,
        (select close_price from price_windows pw
         where pw.asset_id = p.asset_id
         and pw.data_date >= p.ytd_start
         order by pw.data_date limit 1) as start_price
    from price_windows p
    where p.data_date = p.max_date
),

performance_calc as (
    select
        cp.asset_id,
        cp.asset_name,
        cp.asset_type,
        lb.period,
        lb.start_price,
        cp.current_price,
        (cp.current_price / lb.start_price - 1) as return_pct,
        -- Get BTC return for same period
        (select (current_price / start_price - 1)
         from lookback_prices lbb
         inner join current_prices cpb on lbb.asset_id = cpb.asset_id
         where lbb.asset_id = 'BTC'
         and lbb.period = lb.period) as btc_return_pct
    from current_prices cp
    inner join lookback_prices lb
        on cp.asset_id = lb.asset_id
    where lb.start_price is not null
      and lb.start_price > 0
),

final_summary as (
    select
        asset_id,
        asset_name,
        asset_type,
        period,
        start_price,
        current_price,
        return_pct,
        btc_return_pct,
        return_pct - btc_return_pct as return_vs_btc,
        case
            when return_pct > btc_return_pct then 'Outperformed'
            when return_pct = btc_return_pct then 'Matched'
            else 'Underperformed'
        end as vs_btc_status,
        rank() over (partition by period order by return_pct desc) as rank_by_return
    from performance_calc
)

select * from final_summary
order by
    case period
        when '7D' then 1
        when '1M' then 2
        when '3M' then 3
        when '6M' then 4
        when 'YTD' then 5
        when '1Y' then 6
    end,
    rank_by_return
