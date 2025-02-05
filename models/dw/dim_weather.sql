-- depends_on: {{ ref('stg_dim_weather') }}

{{ create_scd_type_2(
    source_table = 'stg_dim_weather'
    , table_name = 'dim_weather'
    , key_col = 'time_readable'
    , timestamp_col = 'transaction_datetime'
    , included_cols = [
        'time_readable',
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
	]
) }}