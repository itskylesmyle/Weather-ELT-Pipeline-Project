{{
    config(
        materialized='incremental',
        unique_key='weather_key'
    )
}}

select
    md5(city_name || '|' || observation_ts::string) as weather_key,
    md5(city_name) as city_key,
    observation_ts::date as date_key,
    observation_ts,
    temperature_f,
    humidity_pct,
    precipitation_in,
    wind_speed_mph,
    loaded_at
from {{ ref('stg_weather_hourly') }}

{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}