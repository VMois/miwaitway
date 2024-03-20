from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from config import BUCKET_NAME, GCP_CONN_ID, RAW_DATASET_NAME, RAW_VEHICLE_TABLE_SCHEMA


TMP_VEHICLE_TABLE_NAME = "tmp_vehicle_position"
VEHICLE_TABLE_NAME = "vehicle_position"


def load_realtime_batch_to_bq(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    objects = gcs_hook.list(BUCKET_NAME, prefix='realtime/vehicle')

    if len(objects):
        load_csv = GCSToBigQueryOperator(
            task_id='gcs_realtime_to_bq',
            bucket=BUCKET_NAME,
            source_objects=objects,
            destination_project_dataset_table=f"{RAW_DATASET_NAME}.{TMP_VEHICLE_TABLE_NAME}",
            autodetect=None,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
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
    create_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=RAW_DATASET_NAME,
        if_exists="ignore",
        gcp_conn_id=GCP_CONN_ID,
    )

    # TODO: if data still exists in tmp table fail the job
    create_raw_tmp_vehicle_table = BigQueryCreateEmptyTableOperator(
        task_id='create_raw_tmp_vehicle_table',
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
        table_id=TMP_VEHICLE_TABLE_NAME,
        schema_fields=RAW_VEHICLE_TABLE_SCHEMA,
        if_exists='ignore',
    )

    load_batch_to_tmp_raw = PythonOperator(
        task_id='load_realtime_batch_to_bq',
        python_callable=load_realtime_batch_to_bq,
    )

    create_raw_vehicle_table = BigQueryCreateEmptyTableOperator(
        task_id='create_raw_vehicle_table',
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
        table_id=VEHICLE_TABLE_NAME,
        schema_fields=RAW_VEHICLE_TABLE_SCHEMA,
        if_exists='ignore',
    )

    append_tmp_to_raw = BigQueryInsertJobOperator(
        task_id='append_tmp_to_raw',
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            'copy': {
                'sourceTable': {
                    'projectId': 'miwaitway',
                    'datasetId': RAW_DATASET_NAME,
                    'tableId': TMP_VEHICLE_TABLE_NAME,
                },
                'destinationTable': {
                    'projectId': 'miwaitway',
                    'datasetId': RAW_DATASET_NAME,
                    'tableId': VEHICLE_TABLE_NAME,
                },
                'createDisposition': 'CREATE_NEVER',
                'writeDisposition': 'WRITE_APPEND',
            }
        }
    )

    create_raw_dataset >> create_raw_tmp_vehicle_table >> load_batch_to_tmp_raw >> create_raw_vehicle_table >> append_tmp_to_raw
