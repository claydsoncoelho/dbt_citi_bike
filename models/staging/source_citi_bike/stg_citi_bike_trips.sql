{# 
    Getting wartermark. 
    If this is the initial load, set default_value to the minimum timestamp available in the table. 
    SELECT MIN(STARTTIME) FROM source; 
#}
{%- set last_extracted_date -%}
    '{{ get_watermark(
        database_name='stg_dw', 
        table_name='int_fact_trips_02', 
        column_name='start_time', 
        default_value=var("citi_bike_start_date")
    ) }}'
{%- endset -%}

with
    source as (select * from {{ source("source_citi_bike", "trips") }}),

    filter_watermark as (
        select
            bikeid AS bike_id,
            starttime AS start_time,
            stoptime AS stop_time,
            tripduration AS trip_duration,
            "START STATION ID" AS start_station_id,
            "START STATION NAME" AS start_station_name,
            "START STATION LATITUDE" AS start_station_latitude,
            "START STATION LONGITUDE" AS start_station_longitude,
            'POINT(' || start_station_longitude || ' ' || start_station_latitude || ')' as start_location,
            "END STATION ID" AS end_station_id,
            "END STATION NAME" AS end_station_name,
            "END STATION LATITUDE" AS end_station_latitude,
            "END STATION LONGITUDE" AS end_station_longitude,
            'POINT(' || end_station_longitude || ' ' || end_station_latitude || ')' as end_location,
            usertype AS user_type,
            "BIRTH YEAR" AS birth_year,
            gender AS gender,
            metadata_filename AS metadata_filename,
            metadata_file_row_number AS metadata_file_row_number,
            metadata_file_last_modified AS metadata_file_last_modified,
            to_varchar(starttime, 'YYYYMM') as start_time_year_month

        from source
        where start_station_latitude > 0 
        and starttime between {{ last_extracted_date }} and dateadd(day, {{ env_var('DBT_EXTRACTION_WINDOW_IN_DAYS') }}, {{ last_extracted_date }})
    ),

    add_columns as (
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
        from filter_watermark
    )


select *
from add_columns
