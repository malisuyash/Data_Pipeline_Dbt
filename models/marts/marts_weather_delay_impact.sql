-- -- KPIs per weather condition + airport

{{ config(materialized='table') }}

-- KPIs per weather condition + airport
SELECT
  Origin,
  weather_condition,
  scheduled_hour,

  COUNT(*) AS total_flights,
  ROUND(AVG(DepDelayMinutes), 2) AS avg_dep_delay,
  ROUND(AVG(ArrDelayMinutes), 2) AS avg_arr_delay,  -- ✅ new column

  ROUND(COUNTIF(delay_bucket != 'On Time') / COUNT(*), 2) AS delay_rate,
  ROUND(COUNTIF(delay_bucket = '>60 min') / COUNT(*), 2) AS long_delay_pct,
  ROUND(COUNTIF(cancel_flag = 1) / COUNT(*), 2) AS cancel_rate  -- ✅ fixed: percentage, not count

FROM {{ ref('fact_flight_delays') }}

GROUP BY Origin, weather_condition, scheduled_hour
