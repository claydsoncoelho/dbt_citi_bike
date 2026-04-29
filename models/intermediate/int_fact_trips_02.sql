{{ config(
    materialized='incremental',
    unique_key=['bike_id', 'start_time'],
) }} 

with

    source as (select * from {{ ref("int_fact_trips_01") }}),
    dim_weather as (select * from {{ ref("dim_weather") }}),
    dim_date as (select * from {{ ref("dim_date") }}),

    -- This cte uses interger part of lat/lon and time of the day to try to find the precise weather.
    find_weather_first_try as (

        select
            source.bike_id,
            source.start_date,
            source.start_time,
            dim_weather_start_location.time_readable,
            dim_date.dim_date_id as dim_date_id_trip,
            dim_weather_start_location.scd_dim_weather_id,
            source.stop_time,
            source.trip_duration,
            source.period_of_day,
            source.trip_distance_meters,
            source.user_type,
            source.age,
            source.age_range,
            source.gender,
            source.birth_year,
            source.start_station_name,
            source.end_station_name,
            source.trip_duration_seconds,
            source.start_station_id,
            source.start_station_latitude,
            source.start_station_longitude,
            source.start_location,
            source.end_station_id,
            source.end_station_latitude,
            source.end_station_longitude,
            source.end_location,
            source.start_lat_bucket,
            source.start_lon_bucket,
            source.start_station_latitude_min,
            source.start_station_latitude_max, 
            source.start_station_longitude_min,
            source.start_station_longitude_max,
            source.start_station_latitude_min_loose,
            source.start_station_latitude_max_loose, 
            source.start_station_longitude_min_loose,
            source.start_station_longitude_max_loose,
            source.start_time_min,
            source.start_time_max

        from source

        left join dim_date on source.trip_start_date = dim_date.date

        left join dim_weather dim_weather_start_location
            on dim_weather_start_location.city_lat_bucket = source.start_lat_bucket  -- EQUALITY to improve performance
            and dim_weather_start_location.city_lon_bucket = source.start_lon_bucket -- EQUALITY to improve performance
            and dim_weather_start_location.date_readable = source.start_date -- EQUALITY to improve performance
            and dim_weather_start_location.time_readable between start_time_min and start_time_max
            
            -- and source.start_time between dim_weather_start_location.scd_valid_from and dim_weather_start_location.scd_valid_to
            -- and dim_weather_start_location.city_latitude between start_station_latitude_min and start_station_latitude_max
            -- and dim_weather_start_location.city_longitude between start_station_longitude_min and start_station_longitude_max
    ),

    weather_not_found as (

        select * from find_weather_first_try where scd_dim_weather_id is null

    ),

    -- This cte uses interger part of lat/lon and date to try to find the weather
    find_weather_second_try as (

        select
            weather_not_found.bike_id,
            weather_not_found.start_date,
            weather_not_found.start_time,
            dim_weather_start_location.date_readable,
            dim_weather_start_location.time_readable,
            weather_not_found.dim_date_id_trip,
            dim_weather_start_location.scd_dim_weather_id,
            weather_not_found.stop_time,
            weather_not_found.trip_duration,
            weather_not_found.period_of_day,
            weather_not_found.trip_distance_meters,
            weather_not_found.user_type,
            weather_not_found.age,
            weather_not_found.age_range,
            weather_not_found.gender,
            weather_not_found.birth_year,
            weather_not_found.start_station_name,
            weather_not_found.end_station_name,
            weather_not_found.trip_duration_seconds,
            weather_not_found.start_station_id,
            weather_not_found.start_station_latitude,
            weather_not_found.start_station_longitude,
            weather_not_found.start_location,
            weather_not_found.end_station_id,
            weather_not_found.end_station_latitude,
            weather_not_found.end_station_longitude,
            weather_not_found.end_location,
            weather_not_found.start_lat_bucket,
            weather_not_found.start_lon_bucket,
            weather_not_found.start_station_latitude_min,
            weather_not_found.start_station_latitude_max, 
            weather_not_found.start_station_longitude_min,
            weather_not_found.start_station_longitude_max,
            weather_not_found.start_station_latitude_min_loose,
            weather_not_found.start_station_latitude_max_loose, 
            weather_not_found.start_station_longitude_min_loose,
            weather_not_found.start_station_longitude_max_loose,
            weather_not_found.start_time_min,
            weather_not_found.start_time_max
            
        from weather_not_found

        -- Join with dim_weather using lat/long and date (loose join to try to fill gaps in Dim_Weather).
        left join dim_weather dim_weather_start_location
            on dim_weather_start_location.city_lat_bucket = weather_not_found.start_lat_bucket  -- EQUALITY to improve performance
            and dim_weather_start_location.city_lon_bucket = weather_not_found.start_lon_bucket -- EQUALITY to improve performance
            and dim_weather_start_location.date_readable = weather_not_found.start_date -- EQUALITY to improve performance

            -- on dim_weather_start_location.city_latitude between weather_not_found.start_station_latitude_min and weather_not_found.start_station_latitude_max
            -- and dim_weather_start_location.city_longitude between weather_not_found.start_station_longitude_min and weather_not_found.start_station_longitude_max
            -- and dim_weather_start_location.date_readable = weather_not_found.start_date
            -- and weather_not_found.start_time between dim_weather_start_location.scd_valid_from and dim_weather_start_location.scd_valid_to

    ),

    merge_both_results as (

        select
            bike_id,
            start_time,
            time_readable,
            dim_date_id_trip,
            scd_dim_weather_id,
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
            start_lat_bucket,
            start_lon_bucket,
            start_station_latitude_min,
            start_station_latitude_max,
            start_station_longitude_min,
            start_station_longitude_max,
            start_station_latitude_min_loose,
            start_station_latitude_max_loose,
            start_station_longitude_min_loose,
            start_station_longitude_max_loose,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location
        from find_weather_first_try where scd_dim_weather_id is not null
        
        union all

        select
            bike_id,
            start_time,
            time_readable,
            dim_date_id_trip,
            scd_dim_weather_id,
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
            start_lat_bucket,
            start_lon_bucket,
            start_station_latitude_min,
            start_station_latitude_max,
            start_station_longitude_min,
            start_station_longitude_max,
            start_station_latitude_min_loose,
            start_station_latitude_max_loose,
            start_station_longitude_min_loose,
            start_station_longitude_max_loose,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location
        from find_weather_second_try

    ),

    prepare_duplicated as (

        select
            bike_id,
            start_time,

            --Difference between the star_trip time and the weather time. 
            --Will be used to choose the best record in case of duplication
            datediff('minute', start_time, time_readable) as trip_weather_time_diff,

            dim_date_id_trip,
            scd_dim_weather_id,
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
            start_lat_bucket,
            start_lon_bucket,
            start_station_latitude_min,
            start_station_latitude_max,
            start_station_longitude_min,
            start_station_longitude_max,
            start_station_latitude_min_loose,
            start_station_latitude_max_loose,
            start_station_longitude_min_loose,
            start_station_longitude_max_loose,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location
        from merge_both_results

    ),


    remove_duplicated as (

        {{ dbt_utils.deduplicate('prepare_duplicated', 'bike_id, start_time', 'trip_weather_time_diff') }}

    )


select *
from remove_duplicated
