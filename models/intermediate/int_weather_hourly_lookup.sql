with weather as (
    select
        airport_code,
        timestamp_trunc(timestamp_utc, hour) as weather_hour,
        temperature_c,
        weather_desc,
        wind_speed_mps,
        visibility_m,
        case 
            when visibility_m < 2000 then 'Severe'
            when wind_speed_mps > 15 then 'High Wind'
            else 'Normal'
        end as weather_severity
    from {{ ref('stg_weather_logs') }}
)

select * from weather
