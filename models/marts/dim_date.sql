with

    source as (select * from {{ ref("seed_dim_date") }})

select *
from source