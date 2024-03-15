import time
import requests
import hashlib
from datetime import datetime, timedelta
from collections import defaultdict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from google.transit import gtfs_realtime_pb2
import polars as pl

from config import BUCKET_NAME, GCP_CONN_ID

CHUNS_TO_LOAD = 10
APPRXIMATE_TIME_TO_LOAD_SECONDS = 10


def get_field_value(obj, field_name: str, default = None):
    try:
        if obj.HasField(field_name):
            return getattr(obj, field_name)
        else:
            return default
    except ValueError:
        return default


def extract_vehicle_location(ti):
    vehicle_location_url = 'https://www.miapp.ca/GTFS_RT/Vehicle/VehiclePositions.pb'
    previous_hash = ti.xcom_pull(key='previous_hash', task_ids='extract_vehicle_location')
    chunks_left = CHUNS_TO_LOAD
    flattened_data = []

    while chunks_left > 0:
        response = requests.get(vehicle_location_url)
        current_hash = hashlib.sha256(response.content).hexdigest()
        if previous_hash:
            if current_hash == previous_hash:
                time.sleep(2)
                continue
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            vehicle = entity.vehicle
            trip = vehicle.trip
            vehicle_data = defaultdict(lambda: None)

            vehicle_data['id'] = entity.id
            vehicle_data['trip_id'] = get_field_value(trip, 'trip_id')
            vehicle_data['route_id'] = get_field_value(trip, 'route_id')
            vehicle_data['direction_id'] = get_field_value(trip, 'direction_id')
            vehicle_data['start_date'] = get_field_value(trip, 'start_date')
            vehicle_data['vehicle_id'] = get_field_value(vehicle.vehicle, 'id')
            vehicle_data['vehicle_label'] = get_field_value(vehicle.vehicle, 'label')
            vehicle_data['latitude'] = get_field_value(vehicle.position, 'latitude')
            vehicle_data['longitude'] = get_field_value(vehicle.position, 'longitude')
            vehicle_data['bearing'] = get_field_value(vehicle.position, 'bearing')
            vehicle_data['speed'] = get_field_value(vehicle.position, 'speed')
            vehicle_data['timestamp'] = get_field_value(vehicle, 'timestamp')
            vehicle_data['occupancy_status'] = get_field_value(vehicle, 'occupancy_status', default=None)
            vehicle_data['occupancy_percentage'] = get_field_value(vehicle, 'occupancy_percentage', default=None)
            flattened_data.append(vehicle_data)
        
        previous_hash = current_hash
        chunks_left -= 1

    df = pl.DataFrame(flattened_data).unique(keep='last')
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    object_path = f'realtime/vehicle_{current_hash}.csv'
    gcs_hook.upload(BUCKET_NAME, object_path, data=df.write_csv(include_header=True))

    ti.xcom_push(key='previous_hash', value=previous_hash)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'extract_realtime_miway_data',
    default_args=default_args,
    description='Extract realtime vehicle location data from MiWay and save them to GCS',
    schedule_interval=timedelta(seconds=CHUNS_TO_LOAD * APPRXIMATE_TIME_TO_LOAD_SECONDS),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['miway', 'realtime'],
    max_active_runs=1,
) as dag:

    extract_vehicle_location = PythonOperator(
        task_id='extract_vehicle_location',
        python_callable=extract_vehicle_location,
        priority_weight=10,
    )
