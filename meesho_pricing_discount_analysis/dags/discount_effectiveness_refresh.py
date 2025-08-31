"""
Airflow DAG to refresh synthetic data and load to Postgres (optional).
- Task 1: Generate data (data_gen.py)
- Task 2: Load to Postgres (db_loader.py) using env var DATABASE_URL
Schedule: daily at 2:30 AM
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "meesho_ba",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="discount_effectiveness_refresh",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval="30 2 * * *",
    catchup=False,
) as dag:

    gen_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command="python3 {{ dag_run.conf.get('repo_path', '.') }}/data_gen.py",
        env={},
    )

    load_pg = BashOperator(
        task_id="load_to_postgres",
        bash_command="python3 {{ dag_run.conf.get('repo_path', '.') }}/db_loader.py --input_dir {{ dag_run.conf.get('repo_path', '.') }}",
        env={"DATABASE_URL": "{{ var.value.DATABASE_URL }}"},
    )

    gen_data >> load_pg
