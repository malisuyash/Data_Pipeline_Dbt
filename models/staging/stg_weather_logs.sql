-- models/staging/stg_weather_logs.sql
-- Standardizes and enriches raw weather data from OpenWeatherMap

SELECT
  airport_code,
  timestamp_utc,
  CAST(EXTRACT(HOUR FROM timestamp_utc) AS INT64) AS weather_hour,

  -- Base weather measurements
  CAST(temperature_c AS FLOAT64) AS temperature_c,
  CAST(wind_speed_mps AS FLOAT64) AS wind_speed_mps,
  CAST(visibility_m AS FLOAT64) AS visibility_m,
  LOWER(weather_desc) AS weather_desc,

  -- ✅ Categorized weather condition
  CASE
    WHEN weather_desc LIKE '%fog%' THEN 'Fog'
    WHEN weather_desc LIKE '%rain%' THEN 'Rain'
    WHEN weather_desc LIKE '%storm%' THEN 'Storm'
    WHEN weather_desc LIKE '%snow%' THEN 'Snow'
    ELSE 'Clear'
  END AS weather_condition,

  -- ✅ Severity bucketing (used in risk scoring)
  CASE
    WHEN visibility_m < 2 THEN 'Severe'
    WHEN wind_speed_mps > 13 THEN 'High Wind'
    ELSE 'Normal'
  END AS weather_severity

FROM {{ source('flight_delay_analytics', 'raw_weather_logs') }}
WHERE airport_code IS NOT NULL
