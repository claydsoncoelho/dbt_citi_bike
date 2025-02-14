# dbt_citi_bike

## Links

https://hub.getdbt.com/

https://dbt-labs.github.io/dbt-project-evaluator/latest/


## dbt commands

dbt docs generate --no-partial-parse

dbt build --no-partial-parse

dbt build --select package:dbt_project_evaluator --no-partial-parse

dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions --no-partial-parse

dbt run --select citi_bike

dbt run-operation get_watermark --args '{database_name: stg_dw,  table_name: int_fact_trips_02,  column_name: start_time,  default_value: 1900-01-01}'

dbt retry

dbt build --select state:modified+