-- Rolling 7-day delay trend by origin

{{ config(materialized='table') }}

SELECT
  FlightDate,
  Airline,
  Origin,
  scheduled_hour,
  AVG(DepDelayMinutes) OVER (
    PARTITION BY Origin
    ORDER BY FlightDate
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7_day_avg_delay,
  COUNT(*) OVER (PARTITION BY Origin) AS flights_last_7_days
FROM {{ ref('fact_flight_delays') }}
