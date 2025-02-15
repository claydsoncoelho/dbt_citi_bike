{% docs col_bike_id %}
Bike ID
{% enddocs %}

{% docs col_start_time %}
Start time of the trip
{% enddocs %}

{% docs col_stop_time %}
End time of the trip
{% enddocs %}

{% docs col_trip_duration %}
Trip duration in seconds
{% enddocs %}

{% docs col_start_station_id %}
Start station ID
{% enddocs %}

{% docs col_start_station_name %}
Start station name
{% enddocs %}

{% docs col_start_station_latitude %}
Latitude of the start station
{% enddocs %}

{% docs col_start_station_longitude %}
Longitude of the start station
{% enddocs %}

{% docs col_start_location %}
Calculated field: combination of the start station latitude and longitude
{% enddocs %}

{% docs col_start_location_geography %}
Geographical representation of the start location (using ST_GeographyFromText)
{% enddocs %}

{% docs col_end_station_id %}
End station ID
{% enddocs %}

{% docs col_end_station_name %}
End station name
{% enddocs %}

{% docs col_end_station_latitude %}
Latitude of the end station
{% enddocs %}

{% docs col_end_station_longitude %}
Longitude of the end station
{% enddocs %}

{% docs col_end_location %}
Calculated field: combination of the end station latitude and longitude
{% enddocs %}

{% docs col_end_location_geography %}
Geographical representation of the end location (using ST_GeographyFromText)
{% enddocs %}

{% docs col_user_type %}
User type (e.g., subscriber or customer)
{% enddocs %}

{% docs col_birth_year %}
Year of birth of the user
{% enddocs %}

{% docs col_gender %}
Gender (Zero=unknown; 1=male; 2=female)
{% enddocs %}

{% docs col_metadata_filename %}
Source data filename
{% enddocs %}

{% docs col_metadata_file_row_number %}
Row number of the record in the file
{% enddocs %}

{% docs col_metadata_file_last_modified %}
Timestamp of the last modification of the file
{% enddocs %}

{% docs col_metadata_processed_flag %}
A flag indicating whether the record has been processed (always set to false in this case)
{% enddocs %}

# #######################################################################################
# Weather NYC
# #######################################################################################

{% docs col_time_readable %}
Human-readable format of the time the data was recorded
{% enddocs %}

{% docs col_city_name %}
Name of the city
{% enddocs %}

{% docs col_country %}
Name of the country the city is located in
{% enddocs %}

{% docs col_city_id %}
Unique identifier for the city
{% enddocs %}

{% docs col_city_findname %}
The name used to find the city in the database
{% enddocs %}

{% docs col_city_latitude %}
Latitude of the city
{% enddocs %}

{% docs col_city_longitude %}
Longitude of the city
{% enddocs %}

{% docs col_city_location %}
Calculated field: combination of the city latitude and longitude
{% enddocs %}

{% docs col_city_location_geography %}
Geographical representation of the city location (using ST_GeographyFromText)
{% enddocs %}

{% docs col_weather_description %}
Detailed description of the weather (e.g., clear sky, rain)
{% enddocs %}

{% docs col_weather_main %}
Main weather condition (e.g., clear, rain, snow)
{% enddocs %}

{% docs col_temperature %}
Temperature in Celsius (or the appropriate unit)
{% enddocs %}

{% docs col_humidity %}
Humidity percentage of the city at the time of data collection
{% enddocs %}

{% docs col_pressure %}
Atmospheric pressure in hPa
{% enddocs %}

{% docs col_wind_speed %}
Wind speed in meters per second
{% enddocs %}

{% docs col_wind_deg %}
Wind direction in degrees
{% enddocs %}
