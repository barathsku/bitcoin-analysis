{{ config(location=generate_external_location()) }}
-- Ideally this should be a view, but due to how DuckDB works with dbt, this needs to be exported to an external table format.

select
    cast(holiday_date as date) as holiday_date,
    holiday_name,
    market_status,
    'US' as market_code
from read_csv_auto('seeds/us_market_holidays.csv')

union all

select
    cast(holiday_date as date) as holiday_date,
    holiday_name,
    market_status,
    'UK' as market_code
from read_csv_auto('seeds/uk_bank_holidays.csv')

union all

select
    cast(holiday_date as date) as holiday_date,
    holiday_name,
    market_status,
    'EU' as market_code
from read_csv_auto('seeds/eu_market_holidays.csv')
