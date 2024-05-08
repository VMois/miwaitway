/*
  The final raw vehicle location table is partitioned by day from timestamp.
  It requires a small transformation from staging to final table.
  Therefore, we have two CREATE TABLE statements here.
*/

CREATE TABLE raw_miway_data.vehicle_position
(
  id STRING OPTIONS (
    description="Non-unique identifier for the vehicle location record"
  ),
  trip_id STRING OPTIONS (
    description="Identifier of the trip the vehicle is serving"
  ),
  route_id STRING OPTIONS (
    description="Identifier of the route the vehicle is serving"
  ),
  direction_id INTEGER OPTIONS (
    description="Direction of travel for the trip"
  ),
  start_date STRING OPTIONS (
    description="The scheduled start date of the trip in YYYYMMDD format"
  ),
  vehicle_id STRING OPTIONS (
    description="Unique identifier of the vehicle"
  ),
  vehicle_label STRING OPTIONS (
    description="Label assigned to the vehicle, such as a number or name"
  ),
  latitude FLOAT64 OPTIONS (
    description="Latitude of the vehicle's position in WGS84"
  ),
  longitude FLOAT64 OPTIONS (
    description="Longitude of the vehicle's position in WGS84"
  ),
  bearing FLOAT64 OPTIONS (
    description="Bearing (direction) of the vehicle in degrees, where 0 is North, 90 is East, 180 is South, and 270 is West"
  ),
  speed FLOAT64 OPTIONS (
    description="Instantaneous speed of the vehicle in meters per second"
  ),
  timestamp TIMESTAMP OPTIONS (
    description="Timestamp of the vehicle location update in Unix epoch time (seconds since January 1, 1970)"
  ),
  occupancy_status INT OPTIONS (
    description="Occupancy status of the vehicle, as defined in the GTFS Realtime specification"
  ),
  occupancy_percentage INT OPTIONS (
    description="Occupancy percentage of the vehicle, as defined in the GTFS Realtime specification"
  )
)
PARTITION BY DATE(timestamp)
OPTIONS(
  description="A table that contains vehicle positions, partitioned by the date extracted from the timestamp.",
  partition_expiration_days=30
);


CREATE TABLE raw_miway_data.stage_vehicle_position
(
  id STRING OPTIONS (
    description="Non-unique identifier for the vehicle location record"
  ),
  trip_id STRING OPTIONS (
    description="Identifier of the trip the vehicle is serving"
  ),
  route_id STRING OPTIONS (
    description="Identifier of the route the vehicle is serving"
  ),
  direction_id INTEGER OPTIONS (
    description="Direction of travel for the trip"
  ),
  start_date STRING OPTIONS (
    description="The scheduled start date of the trip in YYYYMMDD format"
  ),
  vehicle_id STRING OPTIONS (
    description="Unique identifier of the vehicle"
  ),
  vehicle_label STRING OPTIONS (
    description="Label assigned to the vehicle, such as a number or name"
  ),
  latitude FLOAT64 OPTIONS (
    description="Latitude of the vehicle's position in WGS84"
  ),
  longitude FLOAT64 OPTIONS (
    description="Longitude of the vehicle's position in WGS84"
  ),
  bearing FLOAT64 OPTIONS (
    description="Bearing (direction) of the vehicle in degrees, where 0 is North, 90 is East, 180 is South, and 270 is West"
  ),
  speed FLOAT64 OPTIONS (
    description="Instantaneous speed of the vehicle in meters per second"
  ),
  timestamp INT64 OPTIONS (
    description="Timestamp of the vehicle location update in Unix epoch time (seconds since January 1, 1970)"
  ),
  occupancy_status INT OPTIONS (
    description="Occupancy status of the vehicle, as defined in the GTFS Realtime specification"
  ),
  occupancy_percentage INT OPTIONS (
    description="Occupancy percentage of the vehicle, as defined in the GTFS Realtime specification"
  )
)
OPTIONS(
  description="A temporary table that contains vehicle positions.",
);

