{{ config(location=generate_external_location()) }}

select
    cast(holiday_date as date) as holiday_date,
    holiday_name,
    market_status
from read_csv_auto('seeds/us_market_holidays.csv')
