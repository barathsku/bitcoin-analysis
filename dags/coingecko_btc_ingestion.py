from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.core.components import ingest

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='coingecko_btc_ingestion',
    default_args=default_args,
    description='Ingest daily Bitcoin price data from CoinGecko',
    schedule='@daily',
    start_date=datetime(2025, 1, 18),
    catchup=True,
    max_active_runs=1,
    tags=['ingestion', 'coingecko', 'bitcoin'],
) as dag:
    
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')
    
    ingest_btc = ingest(
        source='coingecko',
        resource='btc_usd_daily'
    )
    
    begin >> ingest_btc >> end
