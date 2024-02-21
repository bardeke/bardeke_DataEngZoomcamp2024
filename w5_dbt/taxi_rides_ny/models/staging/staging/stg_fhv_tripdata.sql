
{{ config(materialized='view') }}

with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}
    WHERE EXTRACT(YEAR FROM DATE(pickup_datetime)) = 2019

),

renamed as (

    select
        dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
