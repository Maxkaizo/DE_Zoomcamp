{{ config(materialized="table") }}

WITH trips_data AS (
    SELECT * 
    FROM {{ ref("fact_trips") }}
    WHERE fare_amount > 0 
      AND trip_distance > 0 
      AND payment_type_description IN ('Cash', 'Credit card')
),
percentiles AS (
    SELECT 
        service_type,
        trip_year,
        trip_month,
        -- Percentil 90, 95, 97 con cálculo continuo
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, trip_year, trip_month) AS p90_fare,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, trip_year, trip_month) AS p95_fare,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, trip_year, trip_month) AS p97_fare
    FROM trips_data
)
SELECT DISTINCT * 
FROM percentiles
WHERE trip_year = 2020 AND trip_month = 4
ORDER BY service_type