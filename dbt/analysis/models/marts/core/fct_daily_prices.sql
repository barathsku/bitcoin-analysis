{{ config(location=generate_external_location()) }}

with base_prices as (
    select
        asset_id,
        asset_name,
        asset_type,
        data_date,
        close_price,
        __batch_id,
        __ingested_at
    from {{ ref('int_market__unified_prices') }}
),

-- Forward-fill missing prices (LOCF strategy) with imputation tracking
all_asset_dates as (
    select
        a.asset_id,
        d.date as data_date
    from (select distinct asset_id from base_prices) a
    cross join (select date from {{ ref('dim_dates') }}) d
),

filled_prices as (
    select
        ad.asset_id,
        ad.data_date,
        coalesce(
            bp.close_price,
            last_value(bp.close_price ignore nulls) over (
                partition by ad.asset_id
                order by ad.data_date
                rows between unbounded preceding and current row
            )
        ) as close_price,
        coalesce(
            bp.__batch_id,
            last_value(bp.__batch_id ignore nulls) over (
                partition by ad.asset_id
                order by ad.data_date
                rows between unbounded preceding and current row
            )
        ) as __batch_id,
        coalesce(
            bp.__ingested_at,
            last_value(bp.__ingested_at ignore nulls) over (
                partition by ad.asset_id
                order by ad.data_date
                rows between unbounded preceding and current row
            )
        ) as __ingested_at,
        case
            when bp.close_price is not null then 'OBSERVED'
            else 'IMPUTED'
        end as price_source
    from all_asset_dates ad
    left join base_prices bp
        on ad.asset_id = bp.asset_id
        and ad.data_date = bp.data_date
)

select
    a.asset_id,
    a.asset_name,
    a.asset_type,
    fp.data_date,
    fp.close_price,
    fp.price_source,
    fp.__batch_id,
    fp.__ingested_at
from filled_prices fp
inner join {{ ref('dim_assets') }} a
    on fp.asset_id = a.asset_id
where fp.close_price is not null
order by fp.asset_id, fp.data_date
