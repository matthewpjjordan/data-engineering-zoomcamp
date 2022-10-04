import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

LOCATION = os.environ.get("GCP_LOCATION", "europe-west1")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_to_bq_yellow_mj_dag_v09",
    schedule_interval="@once",
    default_args=default_args,
    max_active_runs=1,
) as dag:

    gcs_refolder_task = GCSToGCSOperator(
        task_id='gcs_refolder_task',
        source_bucket=BUCKET,
        source_object=f'raw/yellow*',
        destination_bucket=BUCKET,
        destination_object=f"yellow/yellow",
        move_object=True,
    )

    create_trip_data_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_trip_data_dataset',
    dataset_id=BIGQUERY_DATASET,
    project_id=PROJECT_ID, 
    location=LOCATION,
    exists_ok=True,
    )

    gcs_to_bq_ext_tbl_task = BigQueryCreateExternalTableOperator(
        task_id="gcs_to_bq_ext_tbl_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"yellow_external",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/yellow/*"],
            },
        },
    )

    CREATE_PART_TBL_QUERY = f'CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.yellow_tripdata_paritioned \
        PARTITION BY DATE(tpep_pickup_datetime) \
        AS SELECT * FROM {BIGQUERY_DATASET}.yellow_external;'

    bq_ext_tbl_to_bq_task = BigQueryInsertJobOperator (
        task_id="bq_ext_tbl_to_bq_tas",
        configuration= {
            "query": {
                "query": CREATE_PART_TBL_QUERY,
                "useLegacySql": False,
            }
        },
    )

    gcs_refolder_task >> create_trip_data_dataset >> gcs_to_bq_ext_tbl_task >> bq_ext_tbl_to_bq_task
