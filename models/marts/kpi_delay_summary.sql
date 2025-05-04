-- models/marts/marts_delay_kpi_summary.sql
-- High-level KPIs grouped by airport, airline, hour, and weather
select
    origin,
    airline,
    weather_condition,
    weather_severity,
    scheduled_hour,

    count(*) as total_flights,
    avg(depdelayminutes) as avg_dep_delay,
    -- avg(arrdelayminutes) as avg_arr_delay,

    -- KPIs
    round(countif(is_delayed = 0) / count(*), 2) as on_time_pct,
    round(countif(delay_bucket = '>60 min') / count(*), 2) as long_delay_pct,
    round(countif(delay_bucket != 'On Time') / count(*), 2) as delay_occurrence_rate

from {{ ref("int_flights_weather_joined") }}
group by origin, airline, weather_condition, weather_severity, scheduled_hour
