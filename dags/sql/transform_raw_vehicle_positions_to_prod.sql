DECLARE var_today STRING "{{ ds }}";
DECLARE var_yesterday STRING "{{ macros.ds_add(ds, -1) }}";
DECLARE var_today_est_limit TIMESTAMP("{{ ds }} 04:00:00 UTC");

MERGE `{{ params.project_id }}.{{ params.prod_dataset_id }}.{{ params.vehicle_table_name }}` AS target
USING (
    SELECT * FROM `{{ params.project_id }}.{{ params.raw_dataset_id }}.{{ params.vehicle_table_name }}` 
    WHERE (TIMESTAMP_TRUNC(timestamp, DAY) = TIMESTAMP(var_today) 
          OR TIMESTAMP_TRUNC(timestamp, DAY) = TIMESTAMP(var_yesterday))
          AND TIMESTAMP_TRUNC(timestamp, HOUR) <= var_today_est_limit)
) AS source
    ON target.vehicle_id = source.vehicle_id
        AND target.timestamp = source.timestamp
WHEN MATCHED THEN
    UPDATE SET
        target.id = source.id
        , target.trip_id = source.trip_id
        , target.route_id = source.route_id
        , target.direction_id = source.direction_id
        , target.start_date = source.start_date
        , target.vehicle_id = source.vehicle_id
        , target.vehicle_label = source.vehicle_label
        , target.location_point = ST_GeogPoint(source.longitude, source.latitude)
        , target.bearing = source.bearing
        , target.speed = source.speed
        , target.timestamp = source.timestamp
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
        , location_point
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
        , ST_GeogPoint(source.longitude, source.latitude)
        , source.bearing
        , source.speed
        , source.timestamp
        , source.occupancy_status
        , source.occupancy_percentage
    );
