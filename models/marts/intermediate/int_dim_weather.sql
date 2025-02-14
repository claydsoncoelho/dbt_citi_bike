{{ config(
    materialized='ephemeral'
) }}

with

    source as (select * from {{ ref("snap_stg_citi_bike_weather_nyc") }}),

    stg_dim as (

        select
            time_readable,
            country,
            city_name,
            weather_main,
            UPPER(SUBSTRING(weather_description, 1, 1)) || SUBSTRING(weather_description, 2) as weather_detail,
            temperature - 273.15 as temperature_celsius,
            humidity,
            wind_speed,
            city_latitude,
            city_longitude,
            city_location,
            ST_GeographyFromText(city_location) as city_location_geography,
            temperature as temperature_kelvin,
            pressure,
            wind_deg,
            city_id,
            city_findname,
            dbt_updated_at
        from source

    )

select *
from stg_dim
