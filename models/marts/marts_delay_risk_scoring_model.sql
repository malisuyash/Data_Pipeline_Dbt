-- Risk classification of Flight based on avg delay + weather
{{ config(materialized='table') }}

SELECT
  Origin,
  Airline,
  weather_condition,
  scheduled_hour,
  avg_dep_delay,
  weather_severity,
  CASE
    WHEN avg_dep_delay > 30 AND weather_severity = 'Severe' THEN 'HIGH'
    WHEN avg_dep_delay > 15 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS delay_risk_score
FROM {{ ref('kpi_delay_summary') }}

