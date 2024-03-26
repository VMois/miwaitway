from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator, BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from config import BUCKET_NAME, GCP_CONN_ID, RAW_DATASET_NAME, PROJECT_ID, STAGE_VEHICLE_TABLE_NAME, VEHICLE_TABLE_NAME


merge_tmp_to_raw_query = f'''
MERGE `{RAW_DATASET_NAME}.{VEHICLE_TABLE_NAME}` AS target
USING `{RAW_DATASET_NAME}.{STAGE_VEHICLE_TABLE_NAME}` AS source
ON target.vehicle_id = source.vehicle_id
AND target.timestamp = source.timestamp
AND target.trip_id = source.trip_id
WHEN MATCHED THEN
    UPDATE SET
        target.id = source.id
        ,target.trip_id = source.trip_id
        ,target.route_id = source.route_id
        ,target.direction_id = source.direction_id
        ,target.start_date = source.start_date
        ,target.vehicle_id = source.vehicle_id
        ,target.vehicle_label = source.vehicle_label
        ,target.latitude = source.latitude
        ,target.longitude = source.longitude
        ,target.bearing = source.bearing
        ,target.speed = source.speed
        ,target.timestamp = source.timestamp
        ,target.occupancy_status = source.occupancy_status
        ,target.occupancy_percentage = source.occupancy_percentage
WHEN NOT MATCHED THEN
    INSERT (
        id
        ,trip_id
        ,route_id
        ,direction_id
        ,start_date
        ,vehicle_id
        ,vehicle_label
        ,latitude
        ,longitude
        ,bearing
        ,speed
        ,timestamp
        ,occupancy_status
        ,occupancy_percentage
    ) VALUES (
        source.id
        ,source.trip_id
        ,source.route_id
        ,source.direction_id
        ,source.start_date
        ,source.vehicle_id
        ,source.vehicle_label
        ,source.latitude
        ,source.longitude
        ,source.bearing
        ,source.speed
        ,source.timestamp
        ,source.occupancy_status
        ,source.occupancy_percentage
    );
'''


def load_realtime_batch_to_bq(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    objects = gcs_hook.list(BUCKET_NAME, prefix='realtime/vehicle')

    if len(objects):
        load_csv = GCSToBigQueryOperator(
            task_id='gcs_realtime_to_bq',
            bucket=BUCKET_NAME,
            source_objects=objects,
            destination_project_dataset_table=f"{RAW_DATASET_NAME}.{STAGE_VEHICLE_TABLE_NAME}",
            autodetect=None,
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
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
    check_if_dataset_exists = BigQueryGetDatasetOperator(
        task_id='check_if_raw_miway_dataset_exists',
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
    )

    check_if_tmp_vehicle_table_has_no_data = BigQueryCheckOperator(
        task_id='check_if_tmp_vehicle_table_has_no_data',
        gcp_conn_id=GCP_CONN_ID,
        sql=f'SELECT NOT EXISTS (SELECT 1 FROM {RAW_DATASET_NAME}.{STAGE_VEHICLE_TABLE_NAME})',
        use_legacy_sql=False,
    )

    check_if_vehicle_table_exists = BigQueryTableExistenceSensor(
        task_id='check_if_vehicle_table_exists',
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
        table_id=VEHICLE_TABLE_NAME,
        poke_interval=1,  
        timeout=1,
        mode='poke',
    )

    load_batch_to_tmp_raw = PythonOperator(
        task_id='load_realtime_batch_to_bq',
        python_callable=load_realtime_batch_to_bq,
    )

    append_tmp_to_raw = BigQueryInsertJobOperator(
        task_id='append_tmp_to_raw',
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            'query': {
                'query': merge_tmp_to_raw_query,
                'useLegacySql': False,
            }
        }
    )

    clean_tmp_table = BigQueryInsertJobOperator(
        task_id='clean_tmp_table',
        gcp_conn_id=GCP_CONN_ID,
        configuration= {
            'query': { 
                'query': f'TRUNCATE TABLE `{RAW_DATASET_NAME}.{STAGE_VEHICLE_TABLE_NAME}`',
                'useLegacySql': False,
            }
        }
    )

    check_if_dataset_exists >> check_if_tmp_vehicle_table_has_no_data >> check_if_vehicle_table_exists >> load_batch_to_tmp_raw >> append_tmp_to_raw >> clean_tmp_table
