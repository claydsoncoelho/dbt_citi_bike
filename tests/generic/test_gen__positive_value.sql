{% test test_gen__positive_value(model, column_name) %}
    select * 
    from {{ model }}
    where {{ column_name }} <=0
{% endtest %}