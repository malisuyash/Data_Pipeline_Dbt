{{ config(
    materialized = 'table'
) }}

WITH latest AS (
  SELECT
    airport_code,
    MAX(timestamp_utc) AS latest_timestamp
  FROM {{ ref('stg_weather_logs') }}
  GROUP BY airport_code
)

SELECT
  w.*
FROM {{ ref('stg_weather_logs') }} w
JOIN latest
  ON w.airport_code = latest.airport_code
  AND w.timestamp_utc = latest.latest_timestamp
