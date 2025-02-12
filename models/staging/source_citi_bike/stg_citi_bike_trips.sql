{{ config(
    materialized='incremental',
    unique_key=['bike_id', 'start_time'],
    post_hook=[
        "delete from {{ this }} 
         where start_time_year_month < (
            select max(start_time_year_month)
            from {{ this }}
         )"
    ]
) }}

with

    source as (select * from {{ source("source_citi_bike", "trips") }}),

    add_new_columns as (

        select
            bikeid AS bike_id,
            starttime AS start_time,
            stoptime AS stop_time,
            tripduration AS trip_duration,
            start_station_id AS start_station_id,
            start_station_name AS start_station_name,
            start_station_latitude AS start_station_latitude,
            start_station_longitude AS start_station_longitude,
            'POINT(' || start_station_longitude || ' ' || start_station_latitude || ')' as start_location,
            end_station_id AS end_station_id,
            end_station_name AS end_station_name,
            end_station_latitude AS end_station_latitude,
            end_station_longitude AS end_station_longitude,
            'POINT(' || end_station_longitude || ' ' || end_station_latitude || ')' as end_location,
            usertype AS user_type,
            birth_year AS birth_year,
            gender AS gender,
            metadata_filename AS metadata_filename,
            metadata_file_row_number AS metadata_file_row_number,
            metadata_file_last_modified AS metadata_file_last_modified,
            to_varchar(starttime, 'YYYYMM') as start_time_year_month

        from source

    ),

    incremental_logic as (

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
            metadata_file_last_modified,
            start_time_year_month
        from add_new_columns

        {% if is_incremental() %}

            where start_time >= (
                select max(start_time) from {{ this }}
            )
            and start_time <= (
                select dateadd(month, 1, max(start_time)) from {{ this }}
            )

        {% else %}

            where start_time_year_month = (
                select min(start_time_year_month) from add_new_columns
            )

        {% endif %}

    )


select *
from incremental_logic
