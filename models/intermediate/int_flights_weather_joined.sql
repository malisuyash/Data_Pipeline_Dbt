-- models/intermediate/int_flight_weather_joined.sql
-- Merges flight data with weather and delay causes

SELECT
  f.*,

  -- Join: cause-based delay info
  c.CARRIER_DELAY,
  c.WEATHER_DELAY,
  c.NAS_DELAY,
  c.SECURITY_DELAY,
  c.LATE_AIRCRAFT_DELAY,
  c.CANCELLATION_CODE,

  -- Join: weather info
  w.weather_condition,
  w.weather_severity,
  w.temperature_c,
  w.wind_speed_mps,
  w.visibility_m

FROM {{ ref('stg_flight_delays') }} f
LEFT JOIN {{ ref('stg_cause_delays') }} c
  ON f.FlightDate = c.FlightDate
  AND f.Origin = c.Origin
  AND f.Dest = c.Dest

LEFT JOIN {{ ref('stg_weather_logs') }} w
  ON f.Origin = w.airport_code
  AND f.scheduled_hour = w.weather_hour
  AND f.FlightDate = DATE(w.timestamp_utc)

