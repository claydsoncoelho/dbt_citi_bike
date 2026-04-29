-- models/intermediate/int_fact_trips_01.sql

{{ config(
    materialized='table'
) }}

{# Getting wartermark... #}
{%- set last_extracted_date -%}
    '{{ get_watermark(
        database_name='stg_dw', 
        table_name='int_fact_trips_02', 
        column_name='start_time', 
        default_value=var("citi_bike_start_date")
    ) }}'
{%- endset -%}

-- Ranges:
-- start_time_min/max: Time 31 min. The weather table has records with interval of around 1 hours.
-- lat/long min/max: 1 degree of latitude corresponds to roughly 111 kilometers.
-- 0.05 degrees is approximately 5.5 kilometers.
-- 0.15 degrees is approximately 16 kilometers.
{%- set time_window -%}
    31
{%- endset -%}
{%- set lat_long_window_tight -%}
    0.05
{%- endset %}
{%- set lat_long_window_loose -%}
    0.15
{%- endset %}


with

    source as (select * from {{ ref("snap_stg_citi_bike_trips") }}),

    prepare_filter as (
        select 
            bike_id,
            start_time,
            cast(start_time as date) as start_date,
            user_type,
            birth_year,
            stop_time,
            start_station_name,
            end_station_name,
            trip_duration,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            -- Since we have only 2 wather stations in NYC with distinct inter lat/lon:
            -- 40.714272	-74.005966
            -- 43.000351	-75.499901
            -- We are rounding the lat/lon bucket to an integer
            round(start_station_latitude) as start_lat_bucket,
            round(start_station_longitude) as start_lon_bucket,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location,
            start_location_geography,
            end_location_geography,
            gender

        from source

        where start_time between {{ last_extracted_date }} and dateadd(day, {{ env_var('DBT_EXTRACTION_WINDOW_IN_DAYS') }}, {{ last_extracted_date }})
    ),

    stg_fact_01 as (
        select
            dateadd(minute, -{{ time_window }}, start_time) as start_time_min,
            dateadd(minute, {{ time_window }}, start_time) as start_time_max,

            -- Trip duration
            time(
                dateadd(second, trip_duration, '1970-01-01 00:00:00')
            ) as trip_duration,

            -- Period of day
            case
                when extract(hour, start_time) between 6 and 11
                then 'Morning'
                when extract(hour, start_time) between 12 and 17
                then 'Afternoon'
                when extract(hour, start_time) between 18 and 20
                then 'Evening'
                else 'Night'
            end as period_of_day,

            -- Trip distance
            st_distance(
                start_location_geography, end_location_geography
            ) as trip_distance_meters,

            -- Age
            birth_year,
            extract(year from start_time) - birth_year as age,

            -- Gender
            case
                gender when 1 then 'Male' when 2 then 'Female' else 'Unknown'
            end as gender,

            date(start_time) as trip_start_date,
            trip_duration as trip_duration_seconds,

            start_station_latitude - {{ lat_long_window_tight }} as start_station_latitude_min,
            start_station_latitude + {{ lat_long_window_tight }} as start_station_latitude_max,
            start_station_longitude - {{ lat_long_window_tight }} as start_station_longitude_min,
            start_station_longitude + {{ lat_long_window_tight }} as start_station_longitude_max,

            start_station_latitude - {{ lat_long_window_loose }} as start_station_latitude_min_loose,
            start_station_latitude + {{ lat_long_window_loose }} as start_station_latitude_max_loose,
            start_station_longitude - {{ lat_long_window_loose }} as start_station_longitude_min_loose,
            start_station_longitude + {{ lat_long_window_loose }} as start_station_longitude_max_loose,

            start_date,
            user_type,
            start_time,
            stop_time,
            bike_id,
            start_station_name,
            end_station_name,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            start_lat_bucket,
            start_lon_bucket,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location
        from prepare_filter
    ),

    stg_fact_02 as (
        select
            start_date,
            start_time_min,
            start_time_max,
            start_station_latitude_min,
            start_station_latitude_max,
            start_station_longitude_min,
            start_station_longitude_max,
            start_station_latitude_min_loose,
            start_station_latitude_max_loose,
            start_station_longitude_min_loose,
            start_station_longitude_max_loose,
            bike_id,
            start_time,
            trip_start_date,
            stop_time,
            trip_duration,
            period_of_day,
            trip_distance_meters,
            user_type,
            age,
            -- Age range
            case
                when age between 0 and 17
                then '0-17'
                when age between 18 and 24
                then '18-24'
                when age between 25 and 34
                then '25-34'
                when age between 35 and 44
                then '35-44'
                when age between 45 and 54
                then '45-54'
                when age between 55 and 64
                then '55-64'
                when age >= 65
                then '65+'
                else 'Unknown'
            end as age_range,

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
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location
        from stg_fact_01
    )


select *
from stg_fact_02
