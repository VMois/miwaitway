from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from jinja2 import Environment, FileSystemLoader

from config import (
    BUCKET_NAME,
    GCP_CONN_ID,
    RAW_DATASET_NAME,
    PROJECT_ID,
    STAGE_VEHICLE_TABLE_NAME,
    VEHICLE_TABLE_NAME,
)


params = {
    "dataset_id": RAW_DATASET_NAME,
    "project_id": PROJECT_ID,
    "stage_vehicle_table_name": STAGE_VEHICLE_TABLE_NAME,
    "vehicle_table_name": VEHICLE_TABLE_NAME,
}


environment = Environment(loader=FileSystemLoader("dags/sql"))
merge_template = environment.get_template("merge_stage_raw_vehicle_position.sql")

merge_query = merge_template.render({"params": params})


def load_realtime_batch_to_bq(**kwargs):
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    objects = gcs_hook.list(BUCKET_NAME, prefix="realtime/vehicle")
    print(objects)
    objects = sorted(objects)
    print(objects)

    for obj in objects:
        load_csv_to_bq = GCSToBigQueryOperator(
            task_id="load_csv_to_bq",
            bucket=BUCKET_NAME,
            source_objects=[obj],
            destination_project_dataset_table=f"{RAW_DATASET_NAME}.{STAGE_VEHICLE_TABLE_NAME}",
            autodetect=None,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            gcp_conn_id=GCP_CONN_ID,
        )
        load_csv_to_bq.execute(context=kwargs)

        bigquery_hook = BigQueryHook(
            gcp_conn_id=GCP_CONN_ID, useLegacySql=False, priority="BATCH"
        )

        configuration = {
            "query": {
                "query": merge_query,
                "useLegacySql": False,
            }
        }

        bigquery_hook.insert_job(configuration, project_id=PROJECT_ID)

        gcs_hook.delete(BUCKET_NAME, object_name=obj)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "load_realtime_miway_data_to_bq",
    default_args=default_args,
    description="Loads realtime vehicle location data from GCS to BigQuery",
    schedule_interval=timedelta(minutes=60),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["miway"],
    max_active_runs=1,
    params=params,
) as dag:
    check_if_dataset_exists = BigQueryGetDatasetOperator(
        task_id="check_if_raw_miway_dataset_exists",
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
    )

    check_if_tmp_vehicle_table_has_no_data = BigQueryCheckOperator(
        task_id="check_if_tmp_vehicle_table_has_no_data",
        gcp_conn_id=GCP_CONN_ID,
        sql=f"SELECT NOT EXISTS (SELECT 1 FROM {RAW_DATASET_NAME}.{STAGE_VEHICLE_TABLE_NAME})",
        use_legacy_sql=False,
    )

    check_if_vehicle_table_exists = BigQueryTableExistenceSensor(
        task_id="check_if_vehicle_table_exists",
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
        table_id=VEHICLE_TABLE_NAME,
        poke_interval=1,
        timeout=1,
        mode="poke",
    )

    load_batch_to_tmp_raw = PythonOperator(
        task_id="load_realtime_batch_to_bq",
        python_callable=load_realtime_batch_to_bq,
    )

    (
        check_if_dataset_exists
        >> check_if_tmp_vehicle_table_has_no_data
        >> check_if_vehicle_table_exists
        >> load_batch_to_tmp_raw
    )
