{{
    config(
        materialized='table'
    )
}}

WITH daily_aggregation AS (
    SELECT
        CAST(tpep_pickup_datetime AS DATE) AS day, 
        COUNT(*) AS trips, 
        SUM(total_amount) AS revenue,
        AVG(fare_amount) AS avg_fare,
        AVG(tip_amount) AS avg_tip
    FROM {{ ref('int_trips_enhanced') }}
    GROUP BY 1
),
moving_averages AS (
    SELECT
        *,
        AVG(trips) OVER (
            ORDER BY day 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_trips_7d_ma
    FROM daily_aggregation
)

SELECT * FROM moving_averages
ORDER BY day