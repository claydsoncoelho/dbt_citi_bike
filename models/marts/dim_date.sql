with source as( select * from {{ ref('int_holidays') }})

select 
    to_varchar(date, 'YYYYMMDD') as dim_date_id,
    date,
    cast(to_varchar(date, 'MM') as int) as month,
    cast(to_varchar(date, 'YYYY') as int) as year,
    monthname(date) as month_name,
    quarter(date) as quarter,
    is_holiday,
    holiday_name

from source