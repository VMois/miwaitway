BEGIN TRANSACTION;

MERGE `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.vehicle_table_name }}` AS target
USING `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.stage_vehicle_table_name }}` AS source
    ON target.vehicle_id = source.vehicle_id
        AND target.timestamp = TIMESTAMP_SECONDS(source.timestamp)
        AND target.trip_id = source.trip_id
WHEN MATCHED THEN
    UPDATE SET
        target.id = source.id
        , target.trip_id = source.trip_id
        , target.route_id = source.route_id
        , target.direction_id = source.direction_id
        , target.start_date = source.start_date
        , target.vehicle_id = source.vehicle_id
        , target.vehicle_label = source.vehicle_label
        , target.latitude = source.latitude
        , target.longitude = source.longitude
        , target.bearing = source.bearing
        , target.speed = source.speed
        , target.timestamp = TIMESTAMP_SECONDS(source.timestamp)
        , target.occupancy_status = source.occupancy_status
        , target.occupancy_percentage = source.occupancy_percentage
WHEN NOT MATCHED THEN
    INSERT (
        id
        , trip_id
        , route_id
        , direction_id
        , start_date
        , vehicle_id
        , vehicle_label
        , latitude
        , longitude
        , bearing
        , speed
        , timestamp
        , occupancy_status
        , occupancy_percentage
    ) VALUES (
        source.id
        , source.trip_id
        , source.route_id
        , source.direction_id
        , source.start_date
        , source.vehicle_id
        , source.vehicle_label
        , source.latitude
        , source.longitude
        , source.bearing
        , source.speed
        , TIMESTAMP_SECONDS(source.timestamp)
        , source.occupancy_status
        , source.occupancy_percentage
    );


TRUNCATE TABLE `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.stage_vehicle_table_name }}`;

COMMIT TRANSACTION;
