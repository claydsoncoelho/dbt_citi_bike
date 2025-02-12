{{ config(
    materialized='incremental',
    unique_key='time_readable'
) }}

with

    source as (select * from {{ source("source_citi_bike", "weather_nyc") }}),

    renamed as (

        select
            -- City details
            DATA:city:name::STRING AS city_name,
            DATA:city:country::STRING AS country,
            DATA:city:id::INT AS city_id,
            DATA:city:findname::STRING AS city_findname,
            
            -- Coordinates from city
            DATA:city:coord:lat::FLOAT AS city_latitude,
            DATA:city:coord:lon::FLOAT AS city_longitude,
            
            -- Weather details
            DATA:weather[0]:description::STRING AS weather_description,
            DATA:weather[0]:main::STRING AS weather_main,
            
            -- Main weather data
            DATA:main:temp::FLOAT AS temperature,
            DATA:main:humidity::INT AS humidity,
            DATA:main:pressure::FLOAT AS pressure,
            
            -- Wind details
            DATA:wind:speed::FLOAT AS wind_speed,
            DATA:wind:deg::FLOAT AS wind_deg,
            
            -- Convert time epoch to readable format
            TO_TIMESTAMP(DATA:time::INT) AS time_readable,

            -- Metadata fields
            metadata_filename AS metadata_filename,
            metadata_file_row_number AS metadata_file_row_number,
            metadata_file_last_modified AS metadata_file_last_modified
        from source 
        {% if incremental %}
            where source.time_readable >= {{ this }}.time_readable
        {% endif %}

    ),

    last_row_number as (
        select
            time_readable,
            max(metadata_file_row_number) as metadata_file_row_number
        from renamed
        group by time_readable
    ),

    location as (

        select
            renamed.time_readable,
            city_name,
            country,
            city_id,
            city_findname,
            city_latitude,
            city_longitude,
            'POINT(' || city_longitude || ' ' || city_latitude || ')' as city_location,
            weather_description,
            weather_main,
            temperature,
            humidity,
            pressure,
            wind_speed,
            wind_deg,
            metadata_filename,
            renamed.metadata_file_row_number,
            metadata_file_last_modified
        from renamed
        inner join last_row_number
        on renamed.time_readable = last_row_number.time_readable
        and renamed.metadata_file_row_number = last_row_number.metadata_file_row_number

    ),

    location_geography as (

        select
            time_readable,
            city_name,
            country,
            city_id,
            city_findname,
            city_latitude,
            city_longitude,
            city_location,
            ST_GeographyFromText(city_location) as city_location_geography,
            weather_description,
            weather_main,
            temperature,
            humidity,
            pressure,
            wind_speed,
            wind_deg,
            metadata_filename,
            metadata_file_row_number,
            metadata_file_last_modified
        from location

    ),

    dedup as (

        {{ dbt_utils.deduplicate('location_geography', 'time_readable', 'metadata_filename') }}

    )


select *
from dedup
