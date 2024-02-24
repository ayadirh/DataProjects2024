with 

source as (

    select * from {{ source('homework', 'external_fhv_tripdata') }}

),

renamed as (

    select
        sr_flag,
        dispatching_base_num,
        affiliated_base_number,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid

    from source

)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
