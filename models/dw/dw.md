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

