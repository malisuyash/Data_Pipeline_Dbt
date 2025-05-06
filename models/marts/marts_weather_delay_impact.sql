-- KPIs per weather condition + airport

{{ config(materialized='table') }}

SELECT
  Origin,
  weather_condition,
  scheduled_hour,
  COUNT(*) AS total_flights,
  AVG(DepDelayMinutes) AS avg_dep_delay,
  COUNTIF(delay_bucket != 'On Time') / COUNT(*) AS delay_rate,
  COUNTIF(delay_bucket = '>60 min') / COUNT(*) AS long_delay_pct,
  COUNTIF(Cancelled = TRUE) / COUNT(*) AS cancel_rate
FROM {{ ref('fact_flight_delays') }}
GROUP BY Origin, weather_condition, scheduled_hour
