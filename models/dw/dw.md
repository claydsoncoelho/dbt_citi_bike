{% docs col_dim_weather_wid %}
Unique identifier for the record in the slowly changing dimension (SCD) table.
{% enddocs %}

{% docs col_dim_weather_sid %}
Natural identifier for the record in the source system table.
{% enddocs %}

{% docs col_weather_detail %}
Detailed weather description with the first letter capitalized.
{% enddocs %}

{% docs col_temperature_celsius %}
Temperature in Celsius, calculated from Kelvin (temperature - 273.15).
{% enddocs %}

{% docs col_temperature_kelvin %}
Temperature in Kelvin.
{% enddocs %}

{% docs col_valid_from %}
Start of the validity period for the record in the slowly changing dimension.
{% enddocs %}

{% docs col_valid_to %}
End of the validity period for the record in the slowly changing dimension.
{% enddocs %}

{% docs col_current_row %}
Flag for the current record in the slowly changing dimension.
{% enddocs %}

{% docs col_age_range %}
The age group of the user. For example, '0-17', '18-24', '25-34', etc.
{% enddocs %}

{% docs col_trip_distance_sum_km %}
Total sum of the trip distances in kilometers. For example, '1.391691853'.
{% enddocs %}

{% docs col_trip_distance_range %}
The range of the trip's distance, usually in kilometers or miles. For example, '0-1'.
{% enddocs %}

{% docs col_period_of_day %}
Categorization of the trip based on the period of day. For example, 'Morning', 'Afternoon', or 'Evening'.
{% enddocs %}

{% docs col_trip_duration_min_range %}
The range of the trip's duration in minutes. For example, '11-20' for a trip that lasted between 11 and 20 minutes.
{% enddocs %}

{% docs col_weather_wid %}
Foreign key referencing the Dim_Weather dimension table.
{% enddocs %}

{% docs col_trip_date_wid %}
Foreign key referencing the Dim_Date` dimension table, representing the date on which the trip occurred. For example, '20150101' for 1st January , 2015.
{% enddocs %}

{% docs col_trip_count %}
The number of trips recorded.
{% enddocs %}
