BUCKET_NAME = 'miwaitway'
GCP_CONN_ID = 'miwaitway_gcp_default'
RAW_DATASET_NAME = 'raw_miway_data'

RAW_VEHICLE_TABLE_SCHEMA = [
    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trip_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "route_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "direction_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "start_date", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "vehicle_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "vehicle_label", "type": "STRING", "mode": "NULLABLE"},  # Assuming vehicle_label should be STRING instead of INTEGER
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "bearing", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "speed", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "occupancy_status", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "occupancy_percentage", "type": "INTEGER", "mode": "NULLABLE"}
]
