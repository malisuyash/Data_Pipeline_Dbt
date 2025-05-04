-- models/staging/stg_cause_delays.sql
-- Delay cause data enrichment

SELECT
  FlightDate,
  ORIGIN AS Origin,
  DEST AS Dest,

  -- Core delay breakdown fields
  CAST(CARRIER_DELAY AS FLOAT64) AS CARRIER_DELAY,
  CAST(WEATHER_DELAY AS FLOAT64) AS WEATHER_DELAY,
  CAST(NAS_DELAY AS FLOAT64) AS NAS_DELAY,
  CAST(SECURITY_DELAY AS FLOAT64) AS SECURITY_DELAY,
  CAST(LATE_AIRCRAFT_DELAY AS FLOAT64) AS LATE_AIRCRAFT_DELAY,

  CANCELLATION_CODE

FROM {{ source('flight_delay_analytics', 'raw_cause_delays') }}
WHERE Origin IS NOT NULL AND Dest IS NOT NULL
