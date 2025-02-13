with

    source as (select * from {{ ref("int_fact_trips_03") }}),

    trans_01 as (

        select
            trip_count,
            dim_trip_date_id,
            scd_dim_weather_id,
            trip_duration_min_range,
            period_of_day,
            trip_distance_range,
            trip_distance_sum_km,
            user_type,
            age_range,
            gender

        from source
    )


select *
from trans_01
