{{ config(materialized="table") }}

with
    fhv_tripdata as (
        select
        dispatching_base_num,
        sr_flag,
        affiliated_base_number,
        pu_locationid,
        do_locationid,
        pickup_datetime,
        dropoff_datetime,        
        'FHV' as service_type
        from {{ ref("stg_homework__external_fhv_tripdata") }}
    ),    
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.pu_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_tripdata.do_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime
from fhv_tripdata
inner join
    dim_zones as pickup_zone 
    on fhv_tripdata.pu_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv_tripdata.do_locationid = dropoff_zone.locationid