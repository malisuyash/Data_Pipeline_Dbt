-- models/marts/marts_delay_kpi_summary.sql
-- KPIs grouped by airport, airline, hour, and weather

{{ config(materialized='table') }}

WITH enriched AS (
  SELECT
    f.Origin,
    a.state_name AS origin_state_name,
    f.Airline,
    al.airline_name,
    f.weather_condition,
    f.weather_severity,
    f.scheduled_hour,
    f.delay_bucket,
    f.is_delayed,
    f.DepDelayMinutes,
    f.ArrDelayMinutes  -- ✅ Add arrival delay here
  FROM {{ ref("int_flights_weather_joined") }} f
  LEFT JOIN {{ ref("dim_airports") }} a
    ON f.Origin = a.airport_code
  LEFT JOIN {{ ref("dim_airlines") }} al
    ON f.Airline = al.airline_name
)

SELECT
  Origin,
  origin_state_name,
  Airline,
  airline_name,
  weather_condition,
  weather_severity,
  scheduled_hour,

  COUNT(*) AS total_flights,
  ROUND(AVG(DepDelayMinutes), 2) AS avg_dep_delay,
  ROUND(AVG(ArrDelayMinutes), 2) AS avg_arr_delay,  

  ROUND(COUNTIF(is_delayed = 0) / COUNT(*), 2) AS on_time_pct,
  ROUND(COUNTIF(delay_bucket = '>60 min') / COUNT(*), 2) AS long_delay_pct,
  ROUND(COUNTIF(delay_bucket != 'On Time') / COUNT(*), 2) AS delay_occurrence_rate

FROM enriched
GROUP BY
  Origin, origin_state_name, Airline, airline_name,
  weather_condition, weather_severity, scheduled_hour

