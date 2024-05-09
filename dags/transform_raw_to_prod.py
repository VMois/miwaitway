import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

from config import (
    GCP_CONN_ID,
    RAW_DATASET_NAME,
    PROJECT_ID,
    VEHICLE_TABLE_NAME,
    PROD_DATASET_NAME,
)


params = {
    "prod_dataset_id": PROD_DATASET_NAME,
    "raw_dataset_id": RAW_DATASET_NAME,
    "project_id": PROJECT_ID,
    "vehicle_table_name": VEHICLE_TABLE_NAME,
}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    os.path.basename(__file__),
    default_args=default_args,
    description="Transform raw stops and vehicles positions data to prod. Runs according to EST timezone as MiWay agency operates there",
    schedule_interval="15 5 * * *",  # schedule is in UTC. In EST it is 1:15AM. We wait a bit to make sure all latest data is loaded.
    start_date=datetime(2024, 5, 8),
    catchup=False,
    tags=["miway", "prod"],
    max_active_runs=1,
    params=params,
) as dag:
    transform_vehicle_positions = BigQueryInsertJobOperator(
        task_id="transform_vehicle_positions",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": "{% include 'sql/transform_raw_vehicle_positions_to_prod.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # task needs to run after raw stops were ingested
    # raw stops are ingested once a day at 00:00 UTC so we should be fine
    # TODO: ideally we would want to have a sensor here that waits for fresh table or static data DAG
    transform_stops = BigQueryInsertJobOperator(
        task_id="transform_stops",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": "{% include 'sql/transform_raw_stops_to_prod.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    transform_vehicle_positions >> transform_stops
