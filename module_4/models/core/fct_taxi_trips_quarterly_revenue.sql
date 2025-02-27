{{ config(materialized="table") }}

WITH trips_data AS (
    SELECT * FROM {{ ref("fact_trips") }}
),
computed_data AS (
    SELECT 
        year_quarter,
        service_type,
        SUM(total_amount) AS quarterly_revenue,
        -- 🔹 Ahora usamos LAG con un offset de 4 para comparar con el mismo trimestre del año anterior
        LAG(SUM(total_amount), 4) OVER (PARTITION BY service_type ORDER BY year_quarter) AS previous_year_revenue,
        -- 🔹 Cálculo del crecimiento YoY correctamente ajustado
        (SUM(total_amount) - LAG(SUM(total_amount), 4) OVER (PARTITION BY service_type ORDER BY year_quarter)) /
        NULLIF(LAG(SUM(total_amount), 4) OVER (PARTITION BY service_type ORDER BY year_quarter), 0) * 100 AS yoy_growth,
        trip_year  -- Lo mantenemos para filtrar después
    FROM trips_data
    GROUP BY year_quarter, service_type, trip_year
)
SELECT * 
FROM computed_data
WHERE trip_year IN (2020)
ORDER BY service_type,yoy_growth desc 






