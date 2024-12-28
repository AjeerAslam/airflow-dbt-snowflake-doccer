{{
    config(materialized='incremental',
            unique_key='id')
}}

select 
    *
from 
    {{ref('source')}}

{% if is_incremental() %}
where name=name
{% endif %}