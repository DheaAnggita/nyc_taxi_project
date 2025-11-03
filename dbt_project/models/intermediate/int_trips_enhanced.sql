{{
    config(
        materialized='view'
    )
}}

with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

zones as (
    select * from {{ ref('stg_taxi_zones') }}
)

select
    t.surrogate_key,
    t.vendorid,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.rate_code_id,
    t.pu_location_id,
    t.do_location_id,
    pu.zone as pu_zone,
    do_.zone as do_zone,
    pu.borough as pu_borough,
    do_.borough as do_borough,
    t.payment_type,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,

    (t.tpep_dropoff_datetime - t.tpep_pickup_datetime) as trip_duration,
    EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) AS trip_duration_sec,
    (t.trip_distance / (EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) / 3600.0)) AS trip_speed_mph,
    CAST(t.tpep_pickup_datetime AS DATE) AS pickup_date,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS pickup_hour,
    CASE 
        WHEN t.trip_distance < 1 THEN 'Short Trip'
        WHEN t.trip_distance >= 10 THEN 'Long Trip'
        ELSE 'Medium Trip'
    END AS trip_category
    
from trips t
left join zones pu on t.pu_location_id = pu.location_id
left join zones do_ on t.do_location_id = do_.location_id
WHERE 
    t.fare_amount > 0 AND 
    t.total_amount > 0 AND
    t.passenger_count > 0