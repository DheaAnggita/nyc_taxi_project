{{
    config(
        materialized='table',
        unique_key='location_id'
    )
}}

SELECT distinct
    location_id,
    borough,
    zone,
    service_zone,
    borough || ' - ' || zone AS borough_zone_name
FROM {{ ref('stg_taxi_zones') }}
WHERE location_id IS NOT NULL