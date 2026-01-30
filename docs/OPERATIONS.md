# Operations Guide

## Accessing the Airflow UI

When the Docker containers are running (via `docker compose up`), the Airflow webserver is exposed locally.

*   **URL**: [http://localhost:8089](http://localhost:8089)
*   **Username**: `airflow`
*   **Password**: `airflow`

The default port was changed from 8080 to 8089 to avoid potential conflicts with other services running on the same port (e.g., colima on Mac using 8080, a different web server, etc.).

## Managing Pipelines (DAGs)

### Enabling DAGs

By default, Airflow DAGs will be paused (grey toggle). To start the scheduled runs:

1.  Locate the DAG in the list (e.g., `coingecko_market_chart_ingestion`).
2.  Click the toggle switch on the left side to **Unpause** it (it will turn blue).
3.  The scheduler will automatically pick up any due runs based on the `start_date` and `schedule_interval`.

### Triggering Manually

To run a DAG immediately, regardless of its schedule:

1.  Click the **Play** button (▶) in the `Actions` column for the desired DAG, or the top right corner of the DAG grid if within the DAG page.
2.  Select **Trigger DAG**.
3.  The run will appear in the DAG grid.

## Standard Pipeline Workflow

The standard daily workflow involves naturally scheduled runs or manual executions of the ingestion DAGs followed by the transformation DAG.

1.  **Ingestion DAGs**:
    *   `coingecko_market_chart_ingestion`
    *   `massive_forex_ingestion`
    *   `massive_stocks_ingestion`

    These DAGs orchestrate the fetching of T-1 data from APIs (Massive & CoinGecko). They are scheduled at 06:00 UTC to account for data availability delays (e.g., free tier API's T-1 restrictions) and ensure data consistency.

2.  **Transformation DAG**:
    *   `dbt_run`

    This DAG orchestrates the dbt models to transform data into analysis-ready datasets. It is triggered at 06:30 UTC every day.

## Manual Ingestion & Backfills

The `manual_ingestion` DAG is a special utility designed for:
*   Backfilling historical data.
*   Re-running specific date ranges (e.g., to fix corrupted data).
*   Ad-hoc ingestion of specific tickers.

### Configuration Parameters

When you click "Trigger DAG" (with config) for `manual_ingestion`, you will be prompted to provide JSON configuration parameters. The DAG accepts the following:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `source` | `string` | `coingecko` | The data source name (must match a valid adapter/contract, e.g., `massive`, `coingecko`). |
| `resource` | `string` | `market_chart` | The specific resource type (e.g., `market_chart`, `stocks`, `forex`). |
| `start_date` | `string` | `2025-01-01` | The start of the ingestion window (YYYY-MM-DD). |
| `end_date` | `string` | `2025-01-31` | The end of the ingestion window (YYYY-MM-DD). |
| `ticker` | `string` | `None` | *Optional*. Specific ticker/symbol to ingest (e.g., `AAPL`, `C:EURUSD`). |
| `force_refetch` | `boolean` | `false` | If `true`, ignores existing data in the staging layer and forces a fresh API call. Useful for overwriting corrupted responses. |

### Example Scenarios

#### 1. Backfilling Bitcoin Data
To ingest Bitcoin market chart data for the first quarter of 2024:

```json
{
  "source": "coingecko",
  "resource": "market_chart",
  "start_date": "2026-01-01",
  "end_date": "2026-01-30",
  "ticker": "bitcoin"
}
```

#### 2. Ad-hoc Stock Ingestion
To ingest Apple (AAPL) stock data for a specific week:

```json
{
  "source": "massive",
  "resource": "stocks",
  "start_date": "2026-01-01",
  "end_date": "2026-01-30",
  "ticker": "AAPL"
}
```

#### 3. Fixing Corrupted Data
If data for Feb 10th was corrupted, force a full data refresh:

```json
{
  "source": "coingecko",
  "resource": "market_chart",
  "start_date": "2026-01-27",
  "end_date": "2026-01-27",
  "force_refetch": true
}
```
