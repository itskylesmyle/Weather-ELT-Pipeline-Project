select *
from {{ ref('fct_weather_hourly') }}
where temperature_f < -94
   or temperature_f > 140