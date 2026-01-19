from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

from common.core.components import ingest

with DAG(
    dag_id='manual_ingestion',
    description='Manual ingestion for backfills and re-runs',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'manual', 'backfill'],
    params={
        'source': Param(
            default='coingecko',
            type='string',
            description='Source name (e.g., coingecko, massive)'
        ),
        'resource': Param(
            default='btc_usd_daily',
            type='string',
            description='Resource name (e.g., btc_usd_daily, stocks_daily)'
        ),
        'start_date': Param(
            default='2025-01-01',
            type='string',
            description='Start date (YYYY-MM-DD)'
        ),
        'end_date': Param(
            default='2025-01-31',
            type='string',
            description='End date (YYYY-MM-DD)'
        ),
        'ticker': Param(
            default='',
            type='string',
            description='Ticker symbol (for Massive stocks/forex, e.g., AAPL, C:EURUSD)'
        ),
    },
) as dag:
    
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')
    
    ingest_manual = ingest(
        source='{{ params.source }}',
        resource='{{ params.resource }}',
        window_start='{{ params.start_date }}',
        window_end='{{ params.end_date }}',
        ticker='{{ params.ticker }}',
    )
    
    begin >> ingest_manual >> end
