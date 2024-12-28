{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

-- Define the CTE for your source data
with source_data as (
    select 
        id,
        name
    from {{ ref('source') }}
),

-- Define existing data only for incremental runs
existing_data as (
    {% if is_incremental() %}
        select 
            id,
            name
        from {{ this }}
    {% else %}
        select 
            null as id, 
            null as name 
        where 1=0  -- Empty result for full refresh
    {% endif %}
),

-- Identify new and updated records
changes as (
    select 
        s.*
    from source_data s
    left join existing_data e
    on s.id = e.id
    where e.id is null  -- New records
       or s.name != e.name  -- Updated records
),

-- Combine changes with existing data
final_data as (
    select * from existing_data  -- Keep all existing rows
    union all
    select * from changes  -- Insert new or updated rows as new entries
)

-- Select the final dataset for insertion
select * from existing_data
