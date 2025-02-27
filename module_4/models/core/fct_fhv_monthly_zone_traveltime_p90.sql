{{ config(materialized="table") }}

WITH trips_data AS (
    SELECT 
        *,
        -- Calcular trip_duration en segundos
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref("dim_fhv_trips") }}
),
p90_calculations AS (
    SELECT 
        trip_year,
        trip_month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        -- Calcular el P90 de trip_duration por origen y destino
        PERCENTILE_CONT(trip_duration, 0.90) OVER (
            PARTITION BY trip_year, trip_month, pickup_locationid, dropoff_locationid
        ) AS p90_trip_duration
    FROM trips_data
),
filtered_data AS (
    SELECT DISTINCT 
        pickup_zone,
        dropoff_zone,
        p90_trip_duration
    FROM p90_calculations
    WHERE trip_year = 2019 
      AND trip_month = 11
      AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')

)
-- Seleccionar el segundo viaje más largo (OFFSET 1)
SELECT * 
FROM filtered_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) <= 2


