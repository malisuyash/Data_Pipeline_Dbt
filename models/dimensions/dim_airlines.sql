-- dim_airlines.sql
-- Standardized airline reference table

SELECT DISTINCT
  Airline AS airline_name
FROM {{ ref('stg_flight_delays') }}