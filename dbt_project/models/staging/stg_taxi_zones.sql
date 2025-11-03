select
    cast(LocationID as integer) as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from {{ source('raw', 'taxi_zone_lookup') }}
WHERE location_id <= 263 
  AND (Borough IS NOT NULL AND TRIM(Borough) != '')