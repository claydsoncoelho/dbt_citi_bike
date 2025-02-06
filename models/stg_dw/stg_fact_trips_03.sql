{{ config(
    materialized='incremental',
    unique_key=['bike_id', 'start_time'],
    post_hook=[
        "update {{ ref('stg_fact_trips_01') }} 
        set metadata_processed_flag = true 
        where metadata_processed_flag = false
        and month_start_trip = (
            select (min(month_start_trip))
            from {{ ref('stg_fact_trips_02') }}
        )"
      ]
) }} 

with

    source as (select * from {{ ref("stg_fact_trips_02") }}),
    dim_weather as (select * from {{ ref("dim_weather") }}),
    dim_date as (select * from {{ ref("dim_date") }}),

    stg_fact_01 as (

        select
            bike_id,
            start_time,

            --Difference between the star_trip time and the weather time. 
            --Will be used to choose the best record in case of duplication
            datediff(
                'minute', 
                source.start_time, 
                dim_weather_start_location.time_readable
            ) as trip_weather_time_diff,

            dim_date.datekey as trip_date_wid,
            dim_weather_start_location.dim_weather_wid as weather_wid,
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
            end_location
        from source

        left join dim_date on source.trip_start_date = dim_date.date

        left join dim_weather dim_weather_start_location
            on dim_weather_start_location.city_latitude between start_station_latitude_min and start_station_latitude_max
            and dim_weather_start_location.city_longitude between start_station_longitude_min and start_station_longitude_max
            and dim_weather_start_location.time_readable between source.start_time_min and start_time_max
    ),

    count_duplicated as (

        select

            row_number() over (
                partition by bike_id, start_time order by trip_weather_time_diff
            ) as copies,

            bike_id,
            start_time,
            trip_date_wid,
            weather_wid,
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
            end_location
        from stg_fact_01

    ),

    remove_duplicated as (

        select
            bike_id,
            start_time,
            trip_date_wid,
            weather_wid,
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
            end_location

        from count_duplicated

        where copies = 1

    )

select *
from remove_duplicated
