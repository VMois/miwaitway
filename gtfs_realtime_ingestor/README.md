# A service to ingest vehicle positions from GTFS Realtime endpoint

Useful Docker command for local testing of vehicle ingestor:

```bash
docker run -v /Users/vmois/Projects/miwaitway/service_account.json:/root/creds/service_account.json -v /Users/vmois/Projects/miwaitway/tmp:/root/tmp -e CHUNKS_TO_LOAD=3 -e LOCAL_STORAGE_PATH="/root/tmp" -e VEHICLE_LOCATION_URL="https://www.miapp.ca/GTFS_RT/Vehicle/VehiclePositions.pb" -e BUCKET_NAME=miwaitway -e LOGLEVEL=debug -e GOOGLE_APPLICATION_CREDENTIALS=/root/creds/service_account.json miwaitway_vehicle_positions_ingestor
```

