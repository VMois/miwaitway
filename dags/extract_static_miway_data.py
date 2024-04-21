import os
import zipfile
import io
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator

from config import RAW_DATASET_NAME, BUCKET_NAME, GCP_CONN_ID

STATIC_GTFS_TMP_PATH = "/tmp/static_gtfs"
MIWAY_URL = "https://www.miapp.ca/GTFS/google_transit.zip"


def download_static_gtfs_data(url: str, output_path: str, ti):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    response = requests.get(url)
    zip_data = zipfile.ZipFile(io.BytesIO(response.content))
    feeds = []
    for filename in zip_data.namelist():
        if filename.endswith(".txt"):
            feed_path = os.path.join(output_path, filename)
            with zip_data.open(filename) as file:
                with open(feed_path, "wb") as f:
                    f.write(file.read())
            feeds.append(feed_path)
    ti.xcom_push(key="feeds", value=feeds)


def upload_feeds_to_gcs(ti, **kwargs):
    feeds = ti.xcom_pull(task_ids="download_static_gtfs_data", key="feeds")
    gcs_feeds = []
    for feed_path in feeds:
        filename = os.path.basename(feed_path)
        gcs_path = f"static/{filename}"
        upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f'upload_{filename.split(".")[0]}_feed_to_gcs',
            src=feed_path,
            dst=gcs_path,
            bucket=BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
        )
        upload_to_gcs.execute(context=kwargs)
        gcs_feeds.append(gcs_path)
    ti.xcom_push(key="gcs_feeds", value=gcs_feeds)


def load_feeds(**kwargs):
    gcs_feeds = kwargs["ti"].xcom_pull(task_ids="upload_feeds_to_gcs", key="gcs_feeds")
    for gcs_path in gcs_feeds:
        filename = os.path.basename(gcs_path)
        table_name = filename.split(".")[0]
        load_csv = GCSToBigQueryOperator(
            task_id=f"gcs_{table_name}_feed_to_bigquery",
            bucket=BUCKET_NAME,
            source_objects=[gcs_path],
            destination_project_dataset_table=f"{RAW_DATASET_NAME}.{table_name}",
            autodetect=True,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            gcp_conn_id=GCP_CONN_ID,
        )
        load_csv.execute(context=kwargs)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "extract_static_miway_data",
    default_args=default_args,
    description="Download static GTFS data, extract feeds, and upload to GCS and BigQuery",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["miway"],
) as dag:
    check_if_dataset_exists = BigQueryGetDatasetOperator(
        task_id="check_if_raw_miway_dataset_exists",
        gcp_conn_id=GCP_CONN_ID,
        dataset_id=RAW_DATASET_NAME,
    )

    download_data = PythonOperator(
        task_id="download_static_gtfs_data",
        python_callable=download_static_gtfs_data,
        op_kwargs={"url": MIWAY_URL, "output_path": STATIC_GTFS_TMP_PATH},
    )

    upload_feeds_to_gcs = PythonOperator(
        task_id="upload_feeds_to_gcs",
        python_callable=upload_feeds_to_gcs,
    )

    load_feeds_into_bigquery = PythonOperator(
        task_id="load_feeds_into_bigquery",
        python_callable=load_feeds,
    )

    (
        check_if_dataset_exists
        >> download_data
        >> upload_feeds_to_gcs
        >> load_feeds_into_bigquery
    )
