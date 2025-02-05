with

    source as (select * from {{ ref("snap_stg_citi_bike_weather_nyc") }}),

    stg_dim as (

        select
            dbt_scd_id,
            time_readable,
            country,
            city_name,
            weather_main,
            weather_description as weather_detail,
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
            dbt_updated_at as transaction_datetime,
            dbt_valid_from,
            dbt_valid_to
        from source

    )

select *
from stg_dim
