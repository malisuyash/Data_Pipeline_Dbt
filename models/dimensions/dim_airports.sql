-- -- models/dimensions/dim_airports.sql
-- -- Enriched airport dimension with city/state info

-- SELECT DISTINCT
--   Origin AS airport_code,
--   origin_city AS city,
--   origin_state AS state_code,
--   origin_state_name AS state_name
-- FROM {{ ref('stg_flight_delays') }}
-- WHERE origin_city IS NOT NULL
--   AND origin_state IS NOT NULL
--   AND origin_state_name IS NOT NULL

-- models/dimensions/dim_airports.sql
-- Cleaned and enriched airport dimension

SELECT DISTINCT
  Origin AS airport_code,
  origin_city AS city,
  origin_state AS state_code,
  origin_state_name AS state_name
FROM {{ ref('stg_flight_delays') }}
WHERE origin_city IS NOT NULL AND origin_city != ''
  AND origin_state IS NOT NULL AND origin_state != ''
  AND origin_state_name IS NOT NULL AND origin_state_name != ''
  AND Origin IS NOT NULL AND Origin != ''
