# dbt_citi_bike

## Links

https://hub.getdbt.com/

https://dbt-labs.github.io/dbt-project-evaluator/latest/



dbt docs generate --no-partial-parse

dbt build --no-partial-parse

dbt build --select package:dbt_project_evaluator --no-partial-parse

dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions --no-partial-parse

dbt run --select citi_bike