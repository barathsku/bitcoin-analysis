from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.core.components import ingest

STOCK_TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'SPY']
FOREX_PAIRS = ['C:EURUSD', 'C:GBPUSD']

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='massive_market_ingestion',
    default_args=default_args,
    description='Ingest daily stock and forex data from Massive',
    schedule='@daily',
    start_date=datetime(2025, 1, 18),
    catchup=True,
    max_active_runs=1,
    tags=['ingestion', 'massive', 'stocks', 'forex'],
) as dag:
    
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')
    
    stock_tasks = []
    for ticker in STOCK_TICKERS:
        task_group = ingest(
            source='massive',
            resource='stocks_daily',
            ticker=ticker
        )
        task_group.group_id = f'ingest_stock_{ticker}'
        stock_tasks.append(task_group)
    
    forex_tasks = []
    for pair in FOREX_PAIRS:
        task_group = ingest(
            source='massive',
            resource='forex_daily',
            ticker=pair
        )
        task_group.group_id = f'ingest_forex_{pair.replace(":", "_")}'
        forex_tasks.append(task_group)
    
    begin >> stock_tasks >> end
    begin >> forex_tasks >> end
