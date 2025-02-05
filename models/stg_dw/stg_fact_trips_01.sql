with

    source as (select * from {{ ref("snap_stg_citi_bike_trips") }}),

    stg_fact_01 as (

        select
            start_time,
            stop_time,

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
            user_type,

            -- Age
            birth_year,
            extract(year from start_time) - birth_year as age,

            -- Gender
            case
                gender when 1 then 'Male' when 2 then 'Female' else 'Unknown'
            end as gender,

            bike_id,
            start_station_name,
            end_station_name,
            st_geographyfromtext(start_location) as start_location_geography,
            st_geographyfromtext(end_location) as end_location_geography,
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

    ),

    stg_fact_02 as (

        select
            start_time,
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
            bike_id,
            birth_year,
            start_station_name,
            end_station_name,
            st_geographyfromtext(start_location) as start_location_geography,
            st_geographyfromtext(end_location) as end_location_geography,
            trip_duration as trip_duration_seconds,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location

        from stg_fact_01
    )

select *
from stg_fact_02
