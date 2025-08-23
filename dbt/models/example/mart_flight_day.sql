
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='flight_id_flight_date',
    partition_by=['flight_date'],
    cluster_by=['route_code']
) }}

WITH fct_booking AS (
    SELECT
        b.dim_flights_key,
        
)

SELECT 
    f.flight_id,
    f.flight_date,
    f.origin,
    f.destination,
    COUNT(*) AS bookings_count
FROM workspace.gold.fact_bookings b
LEFT JOIN workspace.gold.dim_flights f
ON b.dim_flights_key = f.dim_flights_key
GROUP BY f.flight_id,
    f.flight_date,
    f.origin,
    f.destination
