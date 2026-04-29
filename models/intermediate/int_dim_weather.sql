{{ config(
    materialized='ephemeral'
) }}

with

    source as (select * from {{ ref("snap_stg_citi_bike_weather_nyc") }}),

    stg_dim as (

        select
            time_readable,
            cast(time_readable as date) as date_readable,
            country,
            city_name,
            weather_main,
            UPPER(SUBSTRING(weather_description, 1, 1)) || SUBSTRING(weather_description, 2) as weather_detail,
            temperature - 273.15 as temperature_celsius,
            humidity,
            wind_speed,
            cast(city_latitude as NUMBER(9,6)) as city_latitude,
            cast(city_longitude as NUMBER(9,6)) as city_longitude,
            round(cast(city_latitude as NUMBER(9,6))) as city_lat_bucket,
            round(cast(city_longitude as NUMBER(9,6))) as city_lon_bucket,
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
