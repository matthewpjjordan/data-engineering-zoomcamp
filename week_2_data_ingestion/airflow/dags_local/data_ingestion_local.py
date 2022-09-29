from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
URL_TEMPLATE = URL_PREFIX + "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

with DAG (
    dag_id="LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
) as dag:

    wget_task = BashOperator (
        task_id="wget",
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE} && ls -la $AIRFLOW_HOME'
    )

    ingest_task = BashOperator (
        task_id="ingest",
        bash_command=f'ls -la {AIRFLOW_HOME}'
    )

    wget_task >> ingest_task