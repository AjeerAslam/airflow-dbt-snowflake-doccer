{% snapshot snapshot1 %}

{{
    config(
        target_schema='sn',
        unique_key='id',
        strategy='check',
        check_cols= 'all' 
    )
}}

SELECT * FROM {{ source('source','test') }}

{% endsnapshot %}



