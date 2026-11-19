-- Mountain reference site vs. Wasatch Front valley sites, matched by hour.
-- An inversion is when the higher site is warmer than the lower one.

with hourly as (

    select
        f.observation_ts,
        c.city_name,
        c.elevation_ft,
        f.temperature_f,
        f.wind_speed_mph
    from {{ ref('fct_weather_hourly') }} f
    join {{ ref('dim_city') }} c
        on f.city_key = c.city_key

),

mountain as (
    select * from hourly where city_name = 'Park City'
),

valley as (
    select * from hourly where city_name in ('Salt Lake City', 'Provo', 'Lehi')
)

select
    md5(v.city_name || '|' || v.observation_ts::string) as inversion_key,
    v.observation_ts,
    v.observation_ts::date as date_key,
    hour(v.observation_ts) as hour_of_day,
    v.city_name as valley_city,
    v.elevation_ft as valley_elevation_ft,
    m.elevation_ft as mountain_elevation_ft,
    v.temperature_f as valley_temp_f,
    m.temperature_f as mountain_temp_f,
    v.wind_speed_mph as valley_wind_mph,
    -- degrees F per 1,000 ft of elevation gain; a normal day is about -3.6
    round(
        (m.temperature_f - v.temperature_f)
        / nullif(m.elevation_ft - v.elevation_ft, 0) * 1000,
        2
    ) as lapse_rate_f_per_1000ft,
    (m.temperature_f > v.temperature_f) as is_inversion,
    -- an inversion with calm valley wind, so the air is not mixing out
    (m.temperature_f > v.temperature_f and v.wind_speed_mph < 6) as is_stagnant_inversion,
    -- weaker test in case the model mutes inversions: gradient much shallower than normal
    ((m.temperature_f - v.temperature_f)
        / nullif(m.elevation_ft - v.elevation_ft, 0) * 1000 > -1.65) as is_stable_layer
from valley v
join mountain m
    on v.observation_ts = m.observation_ts