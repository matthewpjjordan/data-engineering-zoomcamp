import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
gcs_path = "raw"

table_schema_green_taxi = pa.schema([
    ('VendorID', pa.string()),
    ('lpep_pickup_datetime', pa.timestamp('s')),
    ('lpep_dropoff_datetime', pa.timestamp('s')),
    ('store_and_fwd_flag', pa.string()),
    ('RatecodeID', pa.string()),
    ('PULocationID', pa.int64()),
    ('DOLocationID', pa.int64()),
    ('passenger_count', pa.int64()),
    ('trip_distance', pa.float64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('ehail_fee', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('payment_type', pa.int64()),
    ('trip_type', pa.float64()),
    ('congestion_surcharge', pa.float64()),
])

table_schema_yellow_taxi = pa.schema([
    ('VendorID', pa.string()), 
    ('tpep_pickup_datetime', pa.timestamp('s')), 
    ('tpep_dropoff_datetime', pa.timestamp('s')), 
    ('passenger_count', pa.int64()), 
    ('trip_distance', pa.float64()), 
    ('RatecodeID', pa.string()), 
    ('store_and_fwd_flag', pa.string()), 
    ('PULocationID', pa.int64()), 
    ('DOLocationID', pa.int64()), 
    ('payment_type', pa.int64()), 
    ('fare_amount',pa.float64()), 
    ('extra',pa.float64()), 
    ('mta_tax', pa.float64()), 
    ('tip_amount', pa.float64()), 
    ('tolls_amount', pa.float64()), 
    ('improvement_surcharge', pa.float64()), 
    ('total_amount', pa.float64()), 
    ('congestion_surcharge', pa.float64()),
    ('airport_fee', pa.float64()),
])

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def set_schema(local_file, service):
    table = pq.read_table(f"{local_file}")
    if service == "yellow":
        table = table.cast(table_schema_yellow_taxi)
    elif service == "green":
        table = table.cast(table_schema_green_taxi)
    
    pq.write_table(table, local_file)

def download_upload_dag(
    dag,
    dataset_file,
    url_template,
    path_to_local_home,
    gcs_path,
    service,
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSL {url_template} > {path_to_local_home}/{dataset_file}"
        )

        set_schema_task = PythonOperator(
            task_id="set_schema_task",
            python_callable=set_schema,
            op_kwargs={
                "local_file": f"{path_to_local_home}/{dataset_file}",
                "service": service,
            },
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

        download_dataset_task >> set_schema_task >> local_to_gcs_task >> remove_local_file

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 12, 31),
    "retries": 1,
}

yellow_dataset_file = "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
yellow_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{yellow_dataset_file}"

yellow_taxi_homework_dag_v07 = DAG(
    dag_id="yellow_taxi_homework_dag_v07",
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=yellow_taxi_homework_dag_v07,
    dataset_file=yellow_dataset_file,
    url_template=yellow_dataset_url,
    path_to_local_home=path_to_local_home,
    gcs_path=gcs_path,
    service="yellow"
)

green_dataset_file = "green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
green_dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{green_dataset_file}"

green_taxi_homework_dag_v06 = DAG(
    dag_id="green_taxi_homework_dag_v06",
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_upload_dag(
    dag=green_taxi_homework_dag_v06,
    dataset_file=green_dataset_file,
    url_template=green_dataset_url,
    path_to_local_home=path_to_local_home,
    gcs_path=gcs_path,
    service="green",
)
