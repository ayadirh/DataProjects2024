with
    source as (select * from {{ source("homework", "external_fhv_tripdata") }}),
    renamed as (
        select
            dispatching_base_num,
         
            {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
            as pu_locationid,
            {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
            as do_locationid,

            sr_flag,
            affiliated_base_number,

            -- timestamps
            cast(pickup_datetime as timestamp) as pickup_datetime,
            cast(dropoff_datetime as timestamp) as dropoff_datetime,

        from source
        where extract(year from cast(pickup_datetime as timestamp)) = 2019
    )

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=false) %} limit 100 {% endif %}
