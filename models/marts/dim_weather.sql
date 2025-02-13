
{{ config(
    materialized='table',
    cluster_by=[
        'time_readable',
        'city_latitude',
        'city_longitude'
    ]
) }}  

{{ scd_type_2(
    source_model_name = 'int_dim_weather',
    target_table_name = 'dim_weather',
    key_col = 'time_readable',
    other_cols = [
        'country',
        'city_name',
        'weather_main',
        'weather_detail',
        'temperature_celsius',
        'humidity',
        'wind_speed',
        'city_latitude',
        'city_longitude',
        'city_location',
        'temperature_kelvin',
        'pressure',
        'wind_deg',
        'city_id',
        'city_findname'
	],
    timestamp_col = 'dbt_updated_at'
) }}