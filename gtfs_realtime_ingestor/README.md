# A service to ingest vehicle positions continuously

Useful Docker command for local testing:

```bash
docker run -v /Users/vmois/Projects/miwaitway/service_account.json:/root/creds/service_account.json -e VEHICLE_LOCATION_URL="https://www.miapp.ca/GTFS_RT/Vehicle/VehiclePositions.pb" -e BUCKET_NAME=miwaitway -e LOGLEVEL=debug -e GOOGLE_APPLICATION_CREDENTIALS=/root/creds/service_account.json miwaitway_vehicle_positions_ingestor
```