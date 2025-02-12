{% set old_query %}
  select
    bike_id,
    start_time,
    stop_time
  from {{ ref('stg_citi_bike_trips_01') }}
  where start_time <= '2016-12-31 23:59:56.000'
{% endset %}

{% set new_query %}
  select
    bike_id,
    start_time,
    stop_time
  from {{ ref('int_fact_trips_03') }}
{% endset %}

with result as (
    {{ 
        audit_helper.compare_and_classify_query_results(
            old_query, 
            new_query, 
            primary_key_columns=['bike_id', 'start_time'], 
            columns=['stop_time'],
            sample_limit = 1000000
        )
    }}
)

select * from result
where dbt_audit_row_status <> 'identical'


--  set models_to_generate = codegen.get_models(directory='marts/intermediate', prefix='int_') 

--  codegen.generate_model_yaml(
--     model_names=models_to_generate
-- ) 
