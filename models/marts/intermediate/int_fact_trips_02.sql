{{ config(
    materialized='table',
    cluster_by=[
        'start_time_min',
        'start_time_max',
        'start_station_latitude_min',
        'start_station_latitude_max',
        'start_station_longitude_min',
        'start_station_longitude_max'
    ],
    alias='stg_fact_trips_02'
) }}   

with

    source as (select * from {{ ref("int_fact_trips_01") }}),

    filtered as (

        select 
            start_time_min,
            start_time_max,
            start_station_latitude_min,
            start_station_latitude_max,
            start_station_longitude_min,
            start_station_longitude_max,
            bike_id,
            start_time,
            trip_start_date,
            stop_time,
            trip_duration,
            period_of_day,
            trip_distance_meters,
            user_type,
            age,
            age_range,
            gender,
            birth_year,
            start_station_name,
            end_station_name,
            trip_duration_seconds,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location,
            year_start_trip,
            month_start_trip,
        from source
        where month_start_trip = (
            select min(month_start_trip)
            from source
            where not(metadata_processed_flag)
        )

    )

select *
from filtered
