with

    source as (select * from {{ ref("snap_stg_citi_bike_trips") }}),

    stg_fact as (

        select
            start_time,
            stop_time,
            
            -- Trip duration
            time(DATEADD(SECOND, trip_duration, '1970-01-01 00:00:00')) AS trip_duration,

            -- Period of day
            case
                when extract(hour, start_time) between 6 and 11 then 'Morning'
                when extract(hour, start_time) between 12 and 17 then 'Afternoon'
                when extract(hour, start_time) between 18 and 20 then 'Evening'
                else 'Night'
            end as period_of_day,

            -- Trip distance
            ST_DISTANCE(start_location_geography, end_location_geography) AS trip_distance_meters,
            user_type,
            birth_year,

            -- Gender
            case gender
                when 1 then 'Male'
                when 2 then 'Female'
                else 'Unknown'
            end as gender,

            bike_id,
            start_station_name, 
            end_station_name,           
            ST_GeographyFromText(start_location) as start_location_geography,
            ST_GeographyFromText(end_location) as end_location_geography,
            trip_duration as trip_duration_seconds,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location

        from source
        where dbt_valid_to = to_date('9999-12-31')

    )

select *
from stg_fact
