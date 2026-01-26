from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.core.components import ingest

STOCK_TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'SPY']
FOREX_PAIRS = ['C:EURUSD', 'C:GBPUSD']

default_args = {
    'owner': 'barath',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='massive_market_ingestion',
    default_args=default_args,
    description='Ingest daily stock and forex data from Massive',
    schedule='@daily',
    start_date=datetime(2025, 1, 18),
    catchup=False,  # Range endpoint loads full history; use manual_ingestion for backfills
    max_active_runs=1,
    tags=['ingestion', 'massive', 'stocks', 'forex'],
) as dag:
    
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')
    
    stock_tasks = []
    for ticker in STOCK_TICKERS:
        task_group = ingest(
            group_id=f'ingest_stock_{ticker}',
            source='massive',
            resource='stocks_daily',
            ticker=ticker,
            pool='massive_api_pool'
        )
        stock_tasks.append(task_group)
    
    forex_tasks = []
    for pair in FOREX_PAIRS:
        task_group = ingest(
            group_id=f'ingest_forex_{pair.replace(":", "_")}',
            source='massive',
            resource='forex_daily',
            ticker=pair,
            pool='massive_api_pool'
        )
        forex_tasks.append(task_group)
    
    begin >> stock_tasks >> end
    begin >> forex_tasks >> end
