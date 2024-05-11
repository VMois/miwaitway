CREATE TABLE `miwaitway.prod_miway_data.stops` (
    stop_id STRING OPTIONS(
        description = "The unique identifier for the stop."
    ),
    stop_code INTEGER OPTIONS(
        description = "The public facing code representing the stop."
    ),
    stop_name STRING OPTIONS(
        description = "The official name of the transit stop."
    ),
    tts_stop_name STRING OPTIONS(
        description = "The text-to-speech version of the stop name for accessibility."
    ),
    stop_desc STRING OPTIONS(
        description = "A description of the stop."
    ),
    location_point GEOGRAPHY NOT NULL OPTIONS (
        description="Original lat and long coordinates converted to GEOGRAPHY"
    ),
    zone_id STRING OPTIONS(
        description = "The identifier of the fare zone for the stop."
    ),
    stop_url STRING OPTIONS(
        description = "A URL to more information about the stop."
    ),
    location_type INTEGER OPTIONS(
        description = "The type of location, e.g., station or stop."
    ),
    parent_station STRING OPTIONS(
        description = "The station associated with this stop, if any."
    ),
    stop_timezone STRING OPTIONS(
        description = "The timezone of the stop."
    ),
    wheelchair_boarding INTEGER OPTIONS(
        description = "Indicates if the stop is accessible for wheelchair boarding."
    ),
    level_id STRING OPTIONS(
        description = "The identifier for the level of a multi-story station or stop."
    ),
    platform_code STRING OPTIONS(
        description = "The code of the platform at the stop.")
);

