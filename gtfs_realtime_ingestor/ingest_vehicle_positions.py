"""
Extract realtime vehicle location data from GTFS-realtime and save them to GCS as CSV files.
"""

import os
import time
import requests
import hashlib
import logging
from collections import defaultdict

from google.transit import gtfs_realtime_pb2
from google.cloud import storage
import polars as pl
from typing import Optional


log_level_name = os.getenv("LOGLEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.WARNING)
logging.basicConfig(level=log_level)
logger = logging.getLogger("vehicle_positions_ingestor")


CHUNKS_TO_LOAD = int(os.getenv("CHUNKS_TO_LOAD", "100"))
logger.info(f"CHUNKS_TO_LOAD: {CHUNKS_TO_LOAD}")
if CHUNKS_TO_LOAD is None:
    raise ValueError("CHUNKS_TO_LOAD must be set")

VEHICLE_LOCATION_URL = os.getenv("VEHICLE_LOCATION_URL")
logger.info(f"VEHICLE_LOCATION_URL: {VEHICLE_LOCATION_URL}")
if VEHICLE_LOCATION_URL is None:
    raise ValueError("VEHICLE_LOCATION_URL must be set")

BUCKET_NAME = os.getenv("BUCKET_NAME")
logger.info(f"BUCKET_NAME: {BUCKET_NAME}")
if BUCKET_NAME is None:
    raise ValueError("BUCKET_NAME must be set")


def get_field_value(obj, field_name: str, default=None):
    try:
        if obj.HasField(field_name):
            return getattr(obj, field_name)
        else:
            return default
    except ValueError:
        return default


def extract_vehicle_location():
    logger.info(
        f"Starting extraction of vehicle locations from {VEHICLE_LOCATION_URL}."
    )

    previous_hash: Optional[str] = None
    chunks_left = CHUNKS_TO_LOAD
    flattened_data = []

    logger.info(f'Setting up GCS client for "{BUCKET_NAME}" bucket.')
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)

    logger.info("Starting to load vehicle location data (infinite loop).")
    while True:
        try:
            response = requests.get(VEHICLE_LOCATION_URL)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to load vehicle location data. Retrying in 2 seconds. Error:\n{e}"
            )
            time.sleep(2)
            continue

        current_hash = hashlib.sha256(response.content).hexdigest()
        if previous_hash:
            if current_hash == previous_hash:
                logger.debug(
                    f"Vehicle location data has not changed (hash: {current_hash}). Sleeping for 2 seconds."
                )
                time.sleep(2)
                continue

        previous_hash = current_hash

        logger.debug("Parsing GTFS real-time data.")
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            vehicle = entity.vehicle
            trip = vehicle.trip
            vehicle_data = defaultdict(lambda: None)

            vehicle_data["id"] = entity.id
            vehicle_data["trip_id"] = get_field_value(trip, "trip_id")
            vehicle_data["route_id"] = get_field_value(trip, "route_id")
            vehicle_data["direction_id"] = get_field_value(trip, "direction_id")
            vehicle_data["start_date"] = get_field_value(trip, "start_date")
            vehicle_data["vehicle_id"] = get_field_value(vehicle.vehicle, "id")
            vehicle_data["vehicle_label"] = get_field_value(vehicle.vehicle, "label")
            vehicle_data["latitude"] = get_field_value(vehicle.position, "latitude")
            vehicle_data["longitude"] = get_field_value(vehicle.position, "longitude")
            vehicle_data["bearing"] = get_field_value(vehicle.position, "bearing")
            vehicle_data["speed"] = get_field_value(vehicle.position, "speed")
            vehicle_data["timestamp"] = get_field_value(vehicle, "timestamp")
            vehicle_data["occupancy_status"] = get_field_value(
                vehicle, "occupancy_status", default=None
            )
            vehicle_data["occupancy_percentage"] = get_field_value(
                vehicle, "occupancy_percentage", default=None
            )
            flattened_data.append(vehicle_data)

        chunks_left -= 1
        logger.debug(f"{chunks_left} chunks left before uploading to GCS.")

        if chunks_left == 0:
            chunks_left = CHUNKS_TO_LOAD
            df = pl.DataFrame(flattened_data).unique(keep="last")

            flattened_data.clear()
            object_path = f"realtime/vehicle_{current_hash}.csv"
            logger.debug(f"Uploading chunks to GCS as {object_path}.")
            blob = bucket.blob(object_path)
            blob.upload_from_string(df.write_csv(include_header=True))


if __name__ == "__main__":
    extract_vehicle_location()
