-- -- Main fact table joining flight + weather + delay cause info

-- models/fact/fact_flight_delays.sql
{{ config(materialized='table') }}

SELECT
  f.FlightDate,
  f.Airline,
  f.Origin,
  f.Dest,
  f.scheduled_hour,
  f.DepDelayMinutes,
  f.ArrDelayMinutes,
  f.delay_bucket,
  f.is_delayed,
  f.is_peak_hour,
  CAST(f.Cancelled AS INT64) AS cancel_flag,   -- ✅ updated
  CAST(f.Diverted AS INT64) AS diverted_flag,  -- ✅ updated
  w.weather_condition,
  w.weather_severity,
  c.WEATHER_DELAY,
  c.CARRIER_DELAY
FROM {{ ref('stg_flight_delays') }} f
LEFT JOIN {{ ref('stg_weather_logs') }} w
  ON f.Origin = w.airport_code
  AND f.scheduled_hour = w.weather_hour
  AND f.FlightDate = DATE(w.timestamp_utc)
LEFT JOIN {{ ref('stg_cause_delays') }} c
  ON f.FlightDate = c.FlightDate
  AND f.Origin = c.Origin
  AND f.Dest = c.Dest
