
-- Extracts unique airport locations
SELECT DISTINCT
  Origin AS airport_code
FROM {{ ref('stg_flight_delays') }}

UNION DISTINCT

SELECT DISTINCT
  Dest AS airport_code
FROM {{ ref('stg_flight_delays') }}
