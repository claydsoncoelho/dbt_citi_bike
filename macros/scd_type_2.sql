{%- macro scd_type_2(

    source_model_name,
    target_table_name,
    key_col,
    other_cols,
    timestamp_col
)-%}

with source_table as (
    select *
    from {{ ref(source_model_name) }} 
),

--The difference between row_number()_1 and row_number()_2 of change is used to determine if a change has occurred. 
--If the change value is 0, the records are considered identical.
--If the change value is non-zero, it means the value of the other columns (other_cols) has changed for that particular key_col across different timestamps.
flag_changes as (  
    select 
        {{ key_col }}
        {% for col in other_cols %}, {{ col }} 
        {% endfor -%}
        , {{ timestamp_col }}
        , (
            row_number() over (
                partition by {{ key_col }} order by {{ timestamp_col }}
            ) 
            - 
            row_number() over (
                partition by {{ key_col }} {% for col in other_cols %}, {{ col }} {%- endfor %} order by {{timestamp_col}}
            ) 
        ) as change
    from source_table
),


identify_first_row as (
    select
        {{ key_col }}
        {% for col in other_cols %}, {{ col }}
        {% endfor -%}
        , {{timestamp_col}}
        -- Only run_num = 1 are intersting. If there are more records identic to the first one, we can ignore them.
        , row_number() over (
            partition by {{ key_col }}, change order by {{timestamp_col}}
        ) as row_num
        , row_number() over (
            partition by {{ key_col }} order by {{timestamp_col}}
        ) as first_row_num
    from flag_changes
),


add_valid_from_valid_to as (
    select 
        {{ key_col }}
        {% for col in other_cols %},  {{ col }}
        {% endfor -%}
        , {{ timestamp_col }}
        , case first_row_num
            when 1 then '{{var("min_date")}}'
            else {{ timestamp_col }}
        end as valid_from
        , coalesce(
            lead({{ timestamp_col }}) over (
                partition by {{ key_col }} order by {{ timestamp_col }}
                )
            , '{{var("max_date")}}'
        ) as valid_to
    from identify_first_row
    where row_num = 1
),


add_current_flag as (
    select 
        {{ key_col }}
        {% for col in other_cols %}, {{ col }}
        {% endfor -%}
        , to_timestamp_ntz(valid_from) as scd_valid_from
        , to_timestamp_ntz(valid_to) as scd_valid_to
        , case valid_to
            when '{{ var("max_date") }}' then 'Y'
            else 'N'
        end as scd_current_flag
    from add_valid_from_valid_to

),


final as (
    select 
        {{ dbt_utils.generate_surrogate_key([ key_col, 'scd_valid_from']) }} as scd_{{ target_table_name }}_id
        , {{ key_col }}
        {% for col in other_cols %}, {{ col }}
        {% endfor -%}
        , scd_valid_from
        , scd_valid_to
        , scd_current_flag
    from add_current_flag
)

select * from final

{%- endmacro -%}
