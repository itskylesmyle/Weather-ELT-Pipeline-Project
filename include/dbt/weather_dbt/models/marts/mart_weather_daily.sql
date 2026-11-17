select
    f.city_key,
    c.city_name,
    f.date_key,
    min(f.temperature_f) as min_temp_f,
    max(f.temperature_f) as max_temp_f,
    round(avg(f.temperature_f), 1) as avg_temp_f,
    round(sum(f.precipitation_mm), 1) as total_precip_mm,
    max(f.wind_speed_kmh) as max_wind_kmh,
    count(*) as hours_observed
from {{ ref('fct_weather_hourly') }} f
join {{ ref('dim_city') }} c
    on f.city_key = c.city_key
group by 1, 2, 3