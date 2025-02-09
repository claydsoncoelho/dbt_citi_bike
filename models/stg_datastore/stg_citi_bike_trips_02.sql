{{ config(
    materialized='ephemeral'
) }}

with

    source as (select * from {{ ref("stg_citi_bike_trips_01") }}),

    new_records as (

        select
            bike_id,
            start_time,
            stop_time,
            trip_duration,
            start_station_id,
            start_station_name,
            start_station_latitude,
            start_station_longitude,
            start_location,
            ST_GeographyFromText(start_location) as start_location_geography,
            end_station_id,
            end_station_name,
            end_station_latitude,
            end_station_longitude,
            end_location,
            ST_GeographyFromText(end_location) as end_location_geography,
            user_type,
            birth_year,
            gender,
            metadata_filename,
            metadata_file_row_number,
            metadata_file_last_modified
        from source
        where not(metadata_processed_flag)

    )


select *
from new_records
