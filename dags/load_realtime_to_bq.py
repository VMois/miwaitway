from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from config import BUCKET_NAME, GCP_CONN_ID, DATASET_NAME


def get_field_value(obj, field_name: str, default=None):
    try:
        if obj.HasField(field_name):
            return getattr(obj, field_name)
        else:
            return default
    except ValueError:
        return default


def load_feeds_to_bq(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    objects = gcs_hook.list(BUCKET_NAME, prefix='realtime/vehicle')

    if len(objects):
        load_csv = GCSToBigQueryOperator(
            task_id='gcs_realtime_to_bq',
            bucket=BUCKET_NAME,
            source_objects=objects,
            destination_project_dataset_table=f"{DATASET_NAME}.vehicle_position",
            autodetect=True,
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            gcp_conn_id=GCP_CONN_ID,
        )
        load_csv.execute(context=kwargs)

    # Delete ingested data to preserve space
    for obj in objects:
        gcs_hook.delete(BUCKET_NAME, object_name=obj)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'load_realtime_miway_data_to_bq',
    default_args=default_args,
    description='Loads realtime vehicle location data from GCS to BigQuery',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['miway'],
    max_active_runs=1,
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        if_exists="ignore",
        gcp_conn_id=GCP_CONN_ID,
    )

    load_feeds_into_bigquery = PythonOperator(
        task_id='load_feeds_into_bigquery',
        python_callable=load_feeds_to_bq,
    )

    create_dataset >> load_feeds_into_bigquery
