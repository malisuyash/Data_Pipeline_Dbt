-- FlightDate dimension for calendar views

SELECT DISTINCT
  FlightDate,
  EXTRACT(DAYOFWEEK FROM FlightDate) AS day_of_week,
  EXTRACT(MONTH FROM FlightDate) AS month,
  EXTRACT(YEAR FROM FlightDate) AS year,
  CASE WHEN EXTRACT(DAYOFWEEK FROM FlightDate) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM {{ ref('stg_flight_delays') }}