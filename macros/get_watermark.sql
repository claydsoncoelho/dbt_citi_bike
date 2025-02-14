{# https://docs.getdbt.com/reference/dbt-jinja-functions #}

{% macro get_watermark(database_name, table_name, column_name, default_value) %}

    {%- set source_relation = adapter.get_relation(
        database= database_name ,
        schema= schema ,
        identifier= table_name 
        ) 
    -%}

    {%- set table_exists = source_relation is not none  -%}

    {%- if table_exists -%}

        {%- set query -%}
            select 
                coalesce(
                    cast(max({{ column_name }}) as varchar),
                    '{{ default_value }}'
                ) as {{ column_name }}
            from {{ database_name }}.{{ schema }}.{{ table_name }}
        {%- endset -%}

        {%- set result = run_query(query).columns[0].values()[0] -%}

        {{ log('query = ' ~ query, info=True) }}
        {{ log('result = ' ~ result, info=True) }}

        {{ return(result) }}

    {%- else -%}

        {{ log(database_name ~ '.' ~ schema ~ '.' ~ table_name ~ ' does not exist!', info=True) }}
        {{ return(default_value) }}

    {%- endif -%}
    
{%- endmacro -%}