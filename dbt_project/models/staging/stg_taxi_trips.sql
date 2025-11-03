with raw as (
    select
        VendorID as vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        try_cast(passenger_count as integer) as passenger_count,
        try_cast(trip_distance as double) as trip_distance,
        try_cast(PULocationID as integer) as pu_location_id,
        try_cast(DOLocationID as integer) as do_location_id,
        try_cast(RatecodeID as integer) as rate_code_id,
        store_and_fwd_flag,
        try_cast(payment_type as integer) as payment_type,
        try_cast(fare_amount as double) as fare_amount,
        try_cast(extra as double) as extra,
        try_cast(mta_tax as double) as mta_tax,
        try_cast(tip_amount as double) as tip_amount,
        try_cast(tolls_amount as double) as tolls_amount,
        try_cast(improvement_surcharge as double) as improvement_surcharge,
        try_cast(total_amount as double) as total_amount,
        try_cast(congestion_surcharge as double) as congestion_surcharge,
        try_cast(airport_fee as double) as airport_fee
    from {{ source('raw', 'trips_raw') }}
),
cleaned as (
    select distinct *
    from raw
    where tpep_pickup_datetime is not null
      and tpep_dropoff_datetime is not null
      and fare_amount > 0 and total_amount > 0 and trip_distance > 0 and passenger_count > 0
      and pu_location_id between 1 and 263
      and do_location_id between 1 and 263
),
with_surrogate_key as (
    select
        md5(
            coalesce(cast(vendorid as varchar), '') || '|' ||
            coalesce(cast(tpep_pickup_datetime as varchar), '') || '|' ||
            coalesce(cast(tpep_dropoff_datetime as varchar), '') || '|' ||
            coalesce(cast(pu_location_id as varchar), '') || '|' ||
            coalesce(cast(do_location_id as varchar), '') || '|' ||
            coalesce(cast(trip_distance as varchar), '') 
        ) as surrogate_key,
        *,
        row_number() over (
            partition by 
                vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, 
                pu_location_id, do_location_id, trip_distance 
            order by total_amount 
        ) as rn
    from cleaned
)
select 
    surrogate_key,
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
from with_surrogate_key
where rn = 1