from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.core.components import ingest

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
    dag_id='coingecko_btc_ingestion',
    default_args=default_args,
    description='Ingest daily Bitcoin price data from CoinGecko',
    schedule='@daily',
    start_date=datetime(2025, 1, 18),
    catchup=False,  # Range endpoint loads full history; use manual_ingestion for backfills
    max_active_runs=1,
    tags=['ingestion', 'coingecko', 'bitcoin'],
) as dag:
    
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')
    
    ingest_btc = ingest(
        source='coingecko',
        resource='btc_usd_daily',
        pool='coingecko_api_pool'
    )
    
    begin >> ingest_btc >> end
