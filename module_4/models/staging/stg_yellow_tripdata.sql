{{
    config(
        materialized='view'
    )
}}

select * from {{ source('staging', 'yellow_tripdata') }}