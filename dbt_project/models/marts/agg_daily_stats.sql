{{
    config(
        materialized='table',
        unique_key='day'
    )
}}

select
    day,
    trips,
    revenue,
    avg_fare,
    avg_tip,
    avg_trips_7d_ma
from {{ ref('int_daily_metrics') }}
order by day