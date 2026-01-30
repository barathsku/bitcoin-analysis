from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

from common.core.components import ingest

with DAG(
    dag_id="manual_ingestion",
    description="Manual ingestion for backfills and re-runs",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ingestion", "manual", "backfill"],
    params={
        "source": Param(
            default="coingecko",
            type="string",
            description="Source name (e.g., coingecko, massive)",
        ),
        "resource": Param(
            default="market_chart",
            type="string",
            description="Resource name (e.g., market_chart, stocks, forex)",
        ),
        "start_date": Param(
            default="2025-01-01", type="string", description="Start date (YYYY-MM-DD)"
        ),
        "end_date": Param(
            default="2025-01-31", type="string", description="End date (YYYY-MM-DD)"
        ),
        "ticker": Param(
            default=None,
            type="string",
            description="Ticker symbol (for Massive stocks/forex, e.g., AAPL, C:EURUSD)",
        ),
        "force_refetch": Param(
            default=False,
            type="boolean",
            description="Force re-fetch even if data exists (for corrupted data recovery)",
        ),
    },
    render_template_as_native_obj=True,
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    ingest_manual = ingest(
        source="{{ params.source }}",
        resource="{{ params.resource }}",
        window_start="{{ params.start_date }}",
        window_end="{{ params.end_date }}",
        ticker="{{ params.ticker }}",
        force_refetch="{{ params.force_refetch }}",
    )

    begin >> ingest_manual >> end
