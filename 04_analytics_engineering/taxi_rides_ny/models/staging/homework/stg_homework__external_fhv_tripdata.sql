with

    source as (select * from {{ source("homework", "external_fhv_tripdata") }}),

    renamed as (

        select
            sr_flag,
            dispatching_base_num,
            affiliated_base_number,

            -- pickup_datetime,
            -- dropoff_datetime,
            -- timestamps
            cast(pickup_datetime as timestamp) as pickup_datetime,
            cast(dropoff_datetime as timestamp) as dropoff_datetime,
            
            {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
            as pu_locationid,
            {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
            as do_locationid,
        -- cast(pulocationid as numeric) as pu_locationid,
        -- cast(dolocationid as numeric) as do_locationid
        from source

    )

select *
from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
