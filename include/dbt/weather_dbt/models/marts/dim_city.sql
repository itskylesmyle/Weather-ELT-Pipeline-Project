select
    md5(city_name) as city_key,
    city_name,
    latitude,
    longitude,
    elevation_ft
from {{ ref('stg_weather_hourly') }}
qualify row_number() over (
    partition by city_name
    order by loaded_at desc
) = 1