{% set old_query %}
  select
    bikeid as bike_id,
    starttime as start_time,
    stoptime as stop_time
  from {{ source("source_citi_bike", "trips") }}
  where starttime <= ( select max(start_time) from {{ ref('int_fact_trips_02') }} )
{% endset %}

{% set new_query %}
  select
    bike_id,
    start_time,
    stop_time
  from {{ ref('int_fact_trips_02') }}
{% endset %}

with problems as (
    {{ 
        audit_helper.compare_and_classify_query_results(
            old_query, 
            new_query, 
            primary_key_columns=['bike_id', 'start_time'], 
            columns=['stop_time'],
            sample_limit = 10000000
        )
    }}
)

select * from problems
where dbt_audit_row_status <> 'identical'
order by start_time


--  set models_to_generate = codegen.get_models(directory='marts/intermediate', prefix='int_') 

--  codegen.generate_model_yaml(
--     model_names=models_to_generate
-- ) 
