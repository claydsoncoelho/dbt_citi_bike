with source as(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2020-01-01' as date)"
    )
    }}
)

select 
    to_varchar(date_day, 'YYYYMMDD') as dim_date_id,
    date_day as date,
    cast(to_varchar(date_day, 'MM') as int) as month,
    cast(to_varchar(date_day, 'YYYY') as int) as year,
    monthname(date_day) as month_name,
    quarter(date_day) as quarter

from source