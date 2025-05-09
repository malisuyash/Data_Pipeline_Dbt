-- models/staging/stg_flight_delays.sql

SELECT
  -- Primary fields
  FlightDate,
  Airline,
  Origin,
  Dest,

  -- New fields for dimensions/geography
  OriginCityName AS origin_city,
  OriginState AS origin_state,
  OriginStateName AS origin_state_name,

--   DestCityName AS dest_city,
--   DestState AS dest_state,
--   DestStateName AS dest_state_name,

  -- Standardized and padded scheduled time
  LPAD(CAST(CRSDepTime AS STRING), 4, '0') AS CRSDepTime,
  CAST(DepDelayMinutes AS FLOAT64) AS DepDelayMinutes,
  CAST(ArrDelayMinutes AS FLOAT64) AS ArrDelayMinutes,
  CAST(Cancelled AS BOOL) AS Cancelled,
  CAST(Diverted AS BOOL) AS Diverted,

  -- Engineered timestamp column for scheduled departure
  TIMESTAMP(PARSE_DATETIME('%F %H%M', CONCAT(FlightDate, ' ', LPAD(CRSDepTime, 4, '0')))) AS ScheduledDepartureTime,

  -- Derived hour (used for grouping & weather joining)
  EXTRACT(HOUR FROM TIMESTAMP(PARSE_DATETIME('%F %H%M', CONCAT(FlightDate, ' ', LPAD(CRSDepTime, 4, '0'))))) AS scheduled_hour,

  -- Flag if delayed more than 15 minutes
  CASE
    WHEN DepDelayMinutes >= 15 THEN 1
    ELSE 0
  END AS is_delayed,

  -- Delay bucket for analysis
  CASE
    WHEN DepDelayMinutes < 15 THEN 'On Time'
    WHEN DepDelayMinutes BETWEEN 15 AND 30 THEN '15–30 min'
    WHEN DepDelayMinutes BETWEEN 31 AND 60 THEN '30–60 min'
    ELSE '>60 min'
  END AS delay_bucket,

  -- Peak hour flag (morning 7–10, evening 17–20)
  CASE
    WHEN EXTRACT(HOUR FROM TIMESTAMP(PARSE_DATETIME('%F %H%M', CONCAT(FlightDate, ' ', LPAD(CRSDepTime, 4, '0'))))) BETWEEN 7 AND 10 THEN TRUE
    WHEN EXTRACT(HOUR FROM TIMESTAMP(PARSE_DATETIME('%F %H%M', CONCAT(FlightDate, ' ', LPAD(CRSDepTime, 4, '0'))))) BETWEEN 17 AND 20 THEN TRUE
    ELSE FALSE
  END AS is_peak_hour

FROM `dbt-flight-and-weather.flight_delay_analytics.raw_flight_delays`

