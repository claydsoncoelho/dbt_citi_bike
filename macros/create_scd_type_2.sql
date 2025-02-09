{%- macro create_scd_type_2(
    source_table
    , table_name
    , key_col
    , timestamp_col
    , included_cols
)
-%}

with source_table as (
    select *
    from {{ ref(source_table) }} 
)

, add_ids as (
    select 
        {{ key_col }} as {{ table_name }}_sid
        ,{% for col in included_cols %} {{ col }}
        {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
        , {{timestamp_col}}
    from source_table
)

, dedup as (  
    select 
        {{ table_name }}_sid
        ,{% for col in included_cols %} {{ col }}
        {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
        , {{timestamp_col}}
        -- Used to create SCD type-2, change indicates the previous record did not change.
        , (
            row_number() over (
                partition by {{ table_name }}_sid order by {{timestamp_col}}
            ) 
            - 
            row_number() over (
                partition by {{ table_name }}_sid, {% for col in included_cols %} {{ col }}
            {%- if not loop.last -%}, {%- endif -%}
        {%- endfor %} order by {{timestamp_col}}
            ) 
        ) as change
    from add_ids
)

, identify_first_row as (
    select
        {{ table_name }}_sid
        ,{% for col in included_cols %} {{ col }}
        {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
        , {{timestamp_col}}
        -- Used to exclude rows where there was no change since the last row.
        , row_number() over (
            partition by {{ table_name }}_sid, change order by {{timestamp_col}}
        ) as row_num
        -- Used to identify the first row
        , row_number() over (
            partition by {{ table_name }}_sid order by {{timestamp_col}}
        ) as first_row_num
    from dedup
)

, exclude_and_add_dates as (
    select 
        {{ table_name }}_sid
        ,{% for col in included_cols %} {{ col }}
            {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
        -- Logic to create from date
        , case first_row_num
            when 1 then '{{var("min_date")}}'
            else {{timestamp_col}}
        end as valid_from
        -- Logic to create to date
        , coalesce(
            lead(transaction_datetime) over (
                partition by {{ table_name }}_sid order by {{timestamp_col}}
                )
            , '{{var("max_date")}}'
        ) as valid_to
    from identify_first_row
    where row_num = 1
)

, add_current_column as (
    select 
        {{ table_name }}_sid
        ,{% for col in included_cols %} {{ col }}
        {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
        , to_timestamp_ntz(valid_from) as valid_from
        , to_timestamp_ntz(valid_to) as valid_to
        , case valid_to
            when '{{var("max_date")}}' then 'Y'
            else 'N'
        end as current_row
    from exclude_and_add_dates

)

, final as (
    select 
        {{ dbt_utils.generate_surrogate_key([table_name ~ '_sid', 'valid_from']) }} as {{ table_name }}_wid
        , {{ table_name }}_sid
        ,{% for col in included_cols %} {{ col }}
            {% if not loop.last %}, {%- endif -%}
        {%- endfor -%}
        , valid_from
        , valid_to
        , current_row
    from add_current_column
)

select *
from final

{%- endmacro -%}
