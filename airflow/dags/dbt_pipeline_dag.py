from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "barath",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dbt_pipeline_and_docs",
    default_args=default_args,
    description="Runs dbt build and generates static documentation & data dictionary",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 26),
    catchup=False,
    tags=["dbt", "docs", "reporting"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="/home/airflow/.local/bin/dbt deps --profiles-dir .",
        cwd="/opt/airflow/dbt/analysis",
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt/analysis",
            "DBT_DATA_PATH": "/opt/airflow/data",
        },
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="/home/airflow/.local/bin/dbt build --profiles-dir . --no-partial-parse",
        cwd="/opt/airflow/dbt/analysis",
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt/analysis",
            "DBT_DATA_PATH": "/opt/airflow/data",
        },
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="/home/airflow/.local/bin/dbt docs generate --static --profiles-dir .",
        cwd="/opt/airflow/dbt/analysis",
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt/analysis",
            "DBT_DATA_PATH": "/opt/airflow/data",
        },
    )

    generate_markdown_dictionary = BashOperator(
        task_id="generate_markdown_dictionary",
        bash_command="python /opt/airflow/plugins/generate_data_dictionary.py",
        cwd="/opt/airflow/plugins",
        env={"AIRFLOW_HOME": "/opt/airflow", "PYTHONPATH": "/opt/airflow"},
    )

    copy_dbt_docs = BashOperator(
        task_id="copy_dbt_docs",
        bash_command="cp target/static_index.html /opt/airflow/docs/dbt_docs.html",
        cwd="/opt/airflow/dbt/analysis",
    )

    (
        dbt_deps
        >> dbt_build
        >> dbt_docs_generate
        >> copy_dbt_docs
        >> generate_markdown_dictionary
    )
