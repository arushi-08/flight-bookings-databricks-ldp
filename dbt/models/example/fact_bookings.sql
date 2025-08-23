
{{ config(materialized='table') }}

SELECT * 
FROM workspace.gold.fact_bookings

