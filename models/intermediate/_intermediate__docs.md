{% docs col_start_time_min %}
Start time value minus 120 minutes. Used to join with time_readable of Dim_Weather.
{% enddocs %}

{% docs col_start_time_max %}
Start time value plus 120 minutes. Used to join with time_readable of Dim_Weather.
{% enddocs %}

{% docs col_start_station_latitude_min %}
Start station latitude value minus 0.11 degrees. 0.11 degrees is approximately 12.21 kilometers. Used to join with latitude of Dim_Weather.
{% enddocs %}

{% docs col_start_station_latitude_max %}
Start station latitude value plus 0.11 degrees. 0.11 degrees is approximately 12.21 kilometers. Used to join with latitude of Dim_Weather.
{% enddocs %}

{% docs col_start_station_longitude_min %}
Start station longitude value minus 0.11 degrees. 0.11 degrees is approximately 12.21 kilometers. Used to join with longitude of Dim_Weather.
{% enddocs %}

{% docs col_start_station_longitude_max %}
Start station longitude value plus 0.11 degrees. 0.11 degrees is approximately 12.21 kilometers. Used to join with longitude of Dim_Weather.
{% enddocs %}

{% docs col_trip_start_date %}
Start Time column, without the time part, just the date part.
{% enddocs %}

{% docs col_trip_distance_meters %}
Distance in meters of the trip. Calculated by st_distance Snowflake function.
{% enddocs %}

{% docs col_age %}
Age of the user in the day of the trip.
{% enddocs %}

{% docs col_trip_weather_time_diff %}
Difference between the star_trip time and the weather time. Will be used to choose the best record in case of duplication.
{% enddocs %}

{% docs col_date %}
The actual date value in date format. Example: '2015-01-01'.
{% enddocs %}

{% docs col_is_holiday %}
Indicates if this date is a holiday. Example: 'true'.
{% enddocs %}

{% docs col_holiday_name %}
Holiday name. Null if it is not a holiday. Example: New Year's Day.
{% enddocs %}