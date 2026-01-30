# Data Dictionary

**Last Updated**: 2026-01-30 04:24:35 UTC

This document is automatically generated from dbt artifacts.

## Sources

### Table: `coingecko.market_chart`
**Description**: Bitcoin daily prices from CoinGecko

| Column | Type | Description |
|--------|------|-------------|
| `data_date` | Unknown | Trading date |
| `price_usd` | Unknown | BTC closing price in USD |
| `market_cap_usd` | Unknown | BTC market capitalization in USD |
| `volume_24h_usd` | Unknown | 24-hour trading volume in USD |
| `__batch_id` | Unknown | Ingestion batch identifier |
| `__source_name` | Unknown | Source system name |
| `__ingested_at` | Unknown | Timestamp of ingestion |
| `__schema_version` | Unknown | Schema version hash for evolution tracking |

---

### Table: `massive.forex_daily`
**Description**: Daily forex rates from Massive (Polygon)

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | Unknown | Forex pair ticker (e.g., C:EURUSD) |
| `data_date` | Unknown | Trading date |
| `open` | Unknown | Opening rate |
| `high` | Unknown | Highest rate |
| `low` | Unknown | Lowest rate |
| `close` | Unknown | Closing rate |
| `volume` | Unknown | Trading volume |
| `__batch_id` | Unknown | Ingestion batch identifier |
| `__source_name` | Unknown | Source system name |
| `__ingested_at` | Unknown | Timestamp of ingestion |
| `__schema_version` | Unknown | Schema version hash for evolution tracking |

---

### Table: `massive.stocks_daily`
**Description**: Daily stock prices from Massive (Polygon)

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | Unknown | Stock ticker symbol |
| `data_date` | Unknown | Trading date |
| `open` | Unknown | Opening price |
| `high` | Unknown | Highest price |
| `low` | Unknown | Lowest price |
| `close` | Unknown | Closing price |
| `volume` | Unknown | Trading volume |
| `vwap` | Unknown | Volume-weighted average price |
| `__batch_id` | Unknown | Ingestion batch identifier |
| `__source_name` | Unknown | Source system name |
| `__ingested_at` | Unknown | Timestamp of ingestion |
| `__schema_version` | Unknown | Schema version hash for evolution tracking |

---

## Models

### Marts

### Table: `dim_assets`
**Description**: Asset dimension table

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Unique asset identifier |
| `asset_name` | VARCHAR | Full name of the asset |
| `asset_type` | VARCHAR | Asset classification |

---

### Table: `dim_dates`
**Description**: Date dimension table generated from date spine

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Calendar date |
| `year` | BIGINT | Year of the date |
| `month` | BIGINT | Month of the date (1-12) |
| `day` | BIGINT | Day of the month |
| `day_of_week` | BIGINT | Day of the week (1=Monday, 7=Sunday) |
| `quarter` | BIGINT | Quarter of the year (1-4) |
| `day_of_year` | BIGINT | Day of the year (1-366) |
| `is_trading_day` | INTEGER | Flag indicating weekday (Mon-Fri) |
| `is_us_trading_day` | INTEGER |  |
| `is_uk_trading_day` | INTEGER |  |
| `is_eu_trading_day` | INTEGER |  |
| `is_year_start` | INTEGER | Flag indicating if date is first day of year |
| `year_month` | VARCHAR | Year and month (YYYY-MM) |
| `year_quarter` | VARCHAR | Year and quarter (YYYY-Q#) |

---

### Table: `fct_daily_prices`
**Description**: Fact table of daily closing prices (forward-filled for missing trading days)

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `data_date` | DATE | Price date |
| `close_price` | DOUBLE | Closing price in USD |
| `price_source` | VARCHAR | Whether price was observed from source data or imputed via forward-fill |
| `__batch_id` | VARCHAR | Ingestion batch identifier (forward-filled for traceability) |
| `__ingested_at` | VARCHAR | Timestamp of ingestion (forward-filled) |

---

### Table: `fct_daily_returns`
**Description**: Fact table of daily returns

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `data_date` | DATE | Return date |
| `close_price` | DOUBLE | Closing price on the return date |
| `daily_return` | DOUBLE | Daily return percentage (decimal) |
| `__batch_id` | VARCHAR | Ingestion batch identifier |
| `__ingested_at` | VARCHAR | Timestamp of ingestion |

---

### Table: `fct_rolling_metrics`
**Description**: Fact table with rolling performance metrics

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `data_date` | DATE | Calculation date |
| `close_price` | DOUBLE | Closing price |
| `daily_return` | DOUBLE | daily_return |
| `avg_return_7d` | DOUBLE | 7-day rolling average return |
| `volatility_7d` | DOUBLE | 7-day rolling volatility |
| `obs_count_7d` | BIGINT | Observation count for 7-day window |
| `avg_return_14d` | DOUBLE | 14-day rolling average return |
| `volatility_14d` | DOUBLE | 14-day rolling volatility |
| `obs_count_14d` | BIGINT | Observation count for 14-day window |
| `avg_return_30d` | DOUBLE | 30-day rolling average return |
| `volatility_30d` | DOUBLE | 30-day rolling volatility |
| `obs_count_30d` | BIGINT | Observation count for 30-day window |
| `cumulative_return_from_start` | DOUBLE | Cumulative return from start |

---

### Table: `rpt_correlation`
**Description**: Correlation matrix of asset returns

| Column | Type | Description |
|--------|------|-------------|
| `asset_a` | VARCHAR | First asset in the pair |
| `asset_a_name` | VARCHAR | Name of the first asset |
| `asset_b` | VARCHAR | Second asset in the pair |
| `asset_b_name` | VARCHAR | Name of the second asset |
| `correlation` | DOUBLE | Pearson correlation coefficient |
| `observation_count` | BIGINT | Number of overlapping trading days used for correlation |

---

### Table: `rpt_investment_simulation`
**Description**: Investment simulation comparing $1K lump sum vs $100/month DCA strategy

| Column | Type | Description |
|--------|------|-------------|
| `asset` | VARCHAR | Asset (USD or BTC) |
| `strategy` | VARCHAR | Investment strategy (Lump Sum or DCA) |
| `initial_investment_usd` | DECIMAL(38,1) | Total amount invested in USD |
| `avg_purchase_price` | DOUBLE | Average purchase price of the asset |
| `current_price` | DOUBLE | Current price of the asset |
| `btc_amount` | DOUBLE | Total amount of BTC accumulated |
| `current_value_usd` | DOUBLE | Current portfolio value in USD |
| `profit_loss_usd` | DOUBLE | Profit or loss in USD |
| `return_pct` | DOUBLE | Return on investment percentage |

---

### Table: `rpt_performance_summary`
**Description**: Performance summary showing asset returns across multiple time windows compared to Bitcoin

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `period` | VARCHAR | Time period (7D, 1M, 3M, 6M, YTD, 1Y) |
| `start_price` | DOUBLE | Price at the start of the period |
| `current_price` | DOUBLE | Price at the end of the period |
| `return_pct` | DOUBLE | Total return percentage for the period |
| `btc_return_pct` | DOUBLE | Bitcoin return percentage for the same period |
| `return_vs_btc` | DOUBLE | Performance difference vs Bitcoin (percentage points) |
| `vs_btc_status` | VARCHAR | Whether asset outperformed, matched, or underperformed BTC |
| `rank_by_return` | BIGINT | Rank of asset by return within the period |

---

### Table: `rpt_volatility_comparison`
**Description**: Volatility analysis comparing daily and annualized volatility across assets

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `category` | VARCHAR | Asset category |
| `num_observations` | BIGINT | Number of daily observations |
| `avg_daily_return_pct` | DOUBLE | Average daily return percentage |
| `daily_volatility_pct` | DOUBLE | Standard deviation of daily returns (percentage) |
| `annualized_volatility_pct` | DOUBLE | Annualized volatility (percentage) |
| `min_daily_return_pct` | DOUBLE | Minimum daily return percentage |
| `max_daily_return_pct` | DOUBLE | Maximum daily return percentage |
| `volatility_rank` | BIGINT | Rank of asset by volatility (1 = highest) |

---

### Staging

### Table: `stg_coingecko__market_chart`
**Description**: Bitcoin daily prices from CoinGecko, deduplicated by timestamp

| Column | Type | Description |
|--------|------|-------------|
| `data_date` | DATE | Trading date |
| `data_timestamp` | TIMESTAMP | Timestamp of trading data point |
| `close_price` | DOUBLE | BTC closing price in USD |
| `market_cap_usd` | DOUBLE | BTC market capitalization in USD |
| `volume_usd` | DOUBLE | 24-hour trading volume in USD |
| `asset_id` | VARCHAR | Asset identifier (BTC) |
| `asset_name` | VARCHAR | Asset name (Bitcoin) |
| `asset_type` | VARCHAR | Asset type (crypto) |
| `__batch_id` | VARCHAR | Ingestion batch identifier |
| `__ingested_at` | VARCHAR | Timestamp of ingestion |

---

### Table: `stg_massive__forex_daily`
**Description**: Daily forex rates from Massive (Polygon)

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Currency identifier (e.g., EUR) |
| `data_date` | DATE | Trading date |
| `close_price` | DOUBLE | Closing exchange rate |
| `open_price` | DOUBLE | Opening exchange rate |
| `high_price` | DOUBLE | Highest exchange rate for the day |
| `low_price` | DOUBLE | Lowest exchange rate for the day |
| `volume` | BIGINT | Trading volume |
| `asset_type` | VARCHAR | Asset type (fiat) |
| `asset_name` | VARCHAR | Currency name |
| `raw_ticker` | VARCHAR | Original ticker from source (e.g. C:EURUSD) |
| `__batch_id` | VARCHAR | Ingestion batch identifier |
| `__ingested_at` | VARCHAR | Timestamp of ingestion |

---

### Table: `stg_massive__stocks_daily`
**Description**: Daily stock prices from Massive (Polygon)

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Stock ticker symbol (e.g. AAPL) |
| `data_date` | DATE | Trading date |
| `close_price` | DOUBLE | Closing price |
| `open_price` | DOUBLE | Opening price |
| `high_price` | DOUBLE | Highest price for the day |
| `low_price` | DOUBLE | Lowest price for the day |
| `volume` | DOUBLE | Trading volume |
| `vwap` | DOUBLE | Volume-weighted average price |
| `asset_type` | VARCHAR | Asset type (stock, index) |
| `asset_name` | VARCHAR | Company or Index name |
| `__batch_id` | VARCHAR | Ingestion batch identifier |
| `__ingested_at` | VARCHAR | Timestamp of ingestion |

---

### Table: `stg_static__market_holidays`
**Description**: Static list of market holidays (US and UK)

| Column | Type | Description |
|--------|------|-------------|
| `holiday_date` | DATE | Date of the holiday |
| `holiday_name` | VARCHAR | Name of the holiday |
| `market_status` | VARCHAR | Status of the market (e.g., Closed, Early Close) |
| `market_code` | VARCHAR | Market code (US or UK) |

---

### Other

### Table: `int_market__btc_relative_perf`
**Description**: Compares asset performance relative to Bitcoin

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `data_date` | DATE | Trading date |
| `asset_return` | DOUBLE | Asset daily return |
| `btc_daily_return` | DOUBLE | Bitcoin daily return |
| `return_vs_btc` | DOUBLE | Difference between asset return and BTC return (percentage points) |
| `outperformed_btc_flag` | INTEGER | 1 if asset outperformed BTC, 0 otherwise |

---

### Table: `int_market__daily_returns`
**Description**: Calculates daily returns for each asset

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `data_date` | DATE | Trading date |
| `close_price` | DOUBLE | Closing price |
| `prev_close_price` | DOUBLE | Previous trading day's closing price |
| `daily_return` | DOUBLE | Percentage daily return (decimal) |
| `days_gap` | BIGINT | Number of days since previous observation |
| `__batch_id` | VARCHAR | Ingestion batch identifier |
| `__ingested_at` | VARCHAR | Timestamp of ingestion |

---

### Table: `int_market__data_gaps`
**Description**: Identifies missing dates in source data per asset within the observed date range

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier with missing data |
| `asset_type` | VARCHAR | Type of asset (crypto, fiat, stock, index) |
| `missing_date` | DATE | Date that is missing from source data |
| `is_trading_day` | INTEGER | Whether the missing date falls on a trading day (Mon-Fri) |
| `detected_at` | TIMESTAMP WITH TIME ZONE | Timestamp when the gap was detected |

---

### Table: `int_market__rolling_metrics`
**Description**: Calculates rolling statistics (average return, volatility) over various windows

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Asset identifier |
| `asset_name` | VARCHAR | Asset name |
| `asset_type` | VARCHAR | Asset type |
| `data_date` | DATE | Trading date |
| `close_price` | DOUBLE | Closing price |
| `daily_return` | DOUBLE | Daily return percentage |
| `avg_return_7d` | DOUBLE | 7-day rolling average return |
| `volatility_7d` | DOUBLE | 7-day rolling volatility (stddev) |
| `obs_count_7d` | BIGINT | Count of observations in 7-day window |
| `avg_return_14d` | DOUBLE | 14-day rolling average return |
| `volatility_14d` | DOUBLE | 14-day rolling volatility (stddev) |
| `obs_count_14d` | BIGINT | Count of observations in 14-day window |
| `avg_return_30d` | DOUBLE | 30-day rolling average return |
| `volatility_30d` | DOUBLE | 30-day rolling volatility (stddev) |
| `obs_count_30d` | BIGINT | Count of observations in 30-day window |
| `first_price` | DOUBLE | First observed price for the asset |
| `cumulative_return_from_start` | DOUBLE | Cumulative return since the first observation |

---

### Table: `int_market__unified_prices`
**Description**: Unified price table for all assets (Crypto, Stocks, Forex) including a synthetic USD baseline

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | VARCHAR | Unique asset identifier |
| `asset_name` | VARCHAR | Full name of the asset |
| `asset_type` | VARCHAR | Type of asset (crypto, fiat, stock, index) |
| `data_date` | DATE | Trading date |
| `close_price` | DOUBLE | Closing price or rate |
| `open_price` | DOUBLE | Opening price or rate |
| `high_price` | DOUBLE | Highest price or rate |
| `low_price` | DOUBLE | Lowest price or rate |
| `volume` | DOUBLE | Trading volume |
| `vwap` | DOUBLE | Volume-weighted average price |
| `market_cap_usd` | DOUBLE | Market capitalization in USD (Crypto only) |
| `volume_usd` | DOUBLE | Trading volume in USD (Crypto only) |
| `__batch_id` | VARCHAR | Ingestion batch identifier |
| `__ingested_at` | VARCHAR | Timestamp of ingestion |

---

