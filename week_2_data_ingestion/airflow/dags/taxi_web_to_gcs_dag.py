import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
gcs_path = "raw"

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def download_upload_dag(
    dag,
    dataset_file,
    url_template,
    path_to_local_home,
    gcs_path,    
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSL {url_template} > {path_to_local_home}/{dataset_file}"
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"{gcs_path}/{dataset_file}",
                "local_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        remove_local_file = BashOperator(
            task_id="remove_local_file",
            bash_command=f"rm {path_to_local_home}/{dataset_file}"
        )

        download_dataset_task >> local_to_gcs_task >> remove_local_file

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 12, 31),
    "retries": 1,
}

yellow_dataset_file = "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
yellow_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{yellow_dataset_file}"

yellow_taxi_homework_dag_v05 = DAG(
    dag_id="yellow_taxi_homework_dag_v05",
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=yellow_taxi_homework_dag_v05,
    dataset_file=yellow_dataset_file,
    url_template=yellow_dataset_url,
    path_to_local_home=path_to_local_home,
    gcs_path=gcs_path,
)

green_dataset_file = "green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
green_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{green_dataset_file}"

green_taxi_homework_dag_v05 = DAG(
    dag_id="green_taxi_homework_dag_v05",
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=green_taxi_homework_dag_v05,
    dataset_file=green_dataset_file,
    url_template=green_dataset_url,
    path_to_local_home=path_to_local_home,
    gcs_path=gcs_path,
)

fhv_dataset_file = "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
fhv_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{fhv_dataset_file}"

fhv_taxi_homework_dag_v05 = DAG(
    dag_id="fhv_taxi_homework_dag_v05",
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=fhv_taxi_homework_dag_v05,
    dataset_file=fhv_dataset_file,
    url_template=fhv_dataset_url,
    path_to_local_home=path_to_local_home,
    gcs_path=gcs_path,
)