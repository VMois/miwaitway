"""
Extract realtime vehicle location data from GTFS-realtime and save them to GCS as CSV files.
"""

import os
import time
import requests
import hashlib
import logging
from collections import defaultdict
from pathlib import Path
from typing import Callable, List, Tuple, Optional

import polars as pl
import sentry_sdk
from google.transit import gtfs_realtime_pb2
from google.cloud import storage


sentry_sdk.init()

log_level_name = os.getenv("LOGLEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.WARNING)
logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=log_level)
logger = logging.getLogger("vehicle_positions_ingestor")


def get_field_value(obj, field_name: str, default=None):
    try:
        if obj.HasField(field_name):
            return getattr(obj, field_name)
        else:
            return default
    except ValueError:
        return default


def build_waf_functions(
    data_path: Path, chunks_path: Path, hash_path: Path, default_chunks: int
) -> Tuple[
    Callable[None, Tuple[List, int, Optional[str]]], Callable[[List, int, str], None]
]:
    def load_from_waf() -> Tuple[List, int, Optional[str]]:
        items = []
        if data_path.exists():
            try:
                items = pl.read_csv(data_path).rows(named=True)
            except pl.exceptions.NoDataError:
                logger.debug("WAF data file is empty. Ignoring.")

        chunks_to_load = default_chunks
        if chunks_path.exists():
            with open(chunks_path, "r") as f:
                chunks_to_load = int(f.read())

        hash = None
        if hash_path.exists():
            with open(hash_path, "r") as f:
                hash = str(f.read())

        logger.debug(
            f"Loading from WAF file. Chunks left: {chunks_to_load}, number of items: {len(items)}, hash of latest ProtoBuf file: {hash}"
        )
        return items, chunks_to_load, hash

    def save_to_waf(items: List, chunks_to_load: int, current_hash: str) -> None:
        # TODO: in case of failure in any of the writes, rollback to previous WAF saved or re-try
        df = pl.DataFrame(items)
        with open(data_path, "w") as f:
            df.write_csv(f, include_header=True)

        with open(chunks_path, "w") as f:
            f.write(str(chunks_to_load))

        with open(hash_path, "w") as f:
            f.write(current_hash)
        logger.debug(
            f"Saved to WAF file. Chunks left: {chunks_to_load}, number of items: {len(items)}, hash of latest ProtoBuf file: {current_hash}"
        )

    return load_from_waf, save_to_waf


def extract_vehicle_location():
    logger.info(
        f"Starting extraction of vehicle locations from {VEHICLE_LOCATION_URL}."
    )
    load_from_waf, save_to_waf = build_waf_functions(
        WAF_DATA_PATH, WAF_CHUNKS_PATH, WAF_HASH_PATH, CHUNKS_TO_LOAD
    )
    flattened_data, chunks_left, previous_hash = load_from_waf()

    logger.info(f'Setting up GCS client for "{BUCKET_NAME}" bucket.')
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)

    infer_schema_length = 2000

    logger.info("Starting to load vehicle location data (infinite loop).")
    while True:
        if chunks_left == 0:
            try:
                batch_df = pl.DataFrame(
                    flattened_data, infer_schema_length=infer_schema_length
                ).unique(subset=["vehicle_id", "timestamp", "trip_id"], keep="last")
            except pl.exceptions.ComputeError as e:
                sentry_sdk.capture_exception(e)
                logger.error(f"Potentially infer schema error. Error: {e}")
            else:
                if LOCAL_STORAGE_PATH:
                    logger.debug("Save chunks to a local path.")
                    batch_df.write_csv(
                        file=f"{LOCAL_STORAGE_PATH}/{previous_hash}.csv",
                        include_header=True,
                    )
                else:
                    object_path = f"realtime/vehicle_{previous_hash}.csv"
                    logger.info(f"Uploading chunks to GCS as {object_path}.")
                    blob = bucket.blob(object_path)
                    blob.upload_from_string(batch_df.write_csv(include_header=True))

            finally:
                chunks_left = CHUNKS_TO_LOAD
                flattened_data.clear()
                save_to_waf(flattened_data, chunks_left, previous_hash)

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

        save_to_waf(flattened_data, chunks_left, current_hash)


if __name__ == "__main__":
    CHUNKS_TO_LOAD = os.getenv("CHUNKS_TO_LOAD")
    logger.info(f"CHUNKS_TO_LOAD: {CHUNKS_TO_LOAD}")
    if CHUNKS_TO_LOAD is None:
        raise ValueError("CHUNKS_TO_LOAD must be set")
    CHUNKS_TO_LOAD = int(CHUNKS_TO_LOAD)

    INGESTOR_STORAGE = os.getenv("INGESTOR_STORAGE")
    logger.info(f"INGESTOR_STORAGE: {INGESTOR_STORAGE}")
    if INGESTOR_STORAGE is None:
        raise ValueError("INGESTOR_STORAGE must be set")
    INGESTOR_STORAGE = Path(INGESTOR_STORAGE)

    WAF_DATA_PATH = INGESTOR_STORAGE / "vehicle_positions.csv"
    WAF_CHUNKS_PATH = INGESTOR_STORAGE / "chunks.txt"
    WAF_HASH_PATH = INGESTOR_STORAGE / "hash.txt"

    VEHICLE_LOCATION_URL = os.getenv("VEHICLE_LOCATION_URL")
    logger.info(f"VEHICLE_LOCATION_URL: {VEHICLE_LOCATION_URL}")
    if VEHICLE_LOCATION_URL is None:
        raise ValueError("VEHICLE_LOCATION_URL must be set")

    BUCKET_NAME = os.getenv("BUCKET_NAME")
    logger.info(f"BUCKET_NAME: {BUCKET_NAME}")
    if BUCKET_NAME is None:
        raise ValueError("BUCKET_NAME must be set")

    LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH")
    if LOCAL_STORAGE_PATH:
        logger.info(
            f"Collected files will be saved to a local path {LOCAL_STORAGE_PATH} instead of GCS bucket"
        )
    extract_vehicle_location()
