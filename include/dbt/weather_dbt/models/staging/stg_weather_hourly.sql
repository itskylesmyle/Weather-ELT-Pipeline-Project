with source as (

    select * from {{ source('raw', 'open_meteo_raw') }}

),

flattened as (

    select
        s.city_name,
        s.latitude,
        s.longitude,
        s.data_date,
        s.payload:elevation::float as elevation_m,
        s.loaded_at,
        h.value::timestamp_ntz as observation_ts,
        round(s.payload:hourly:temperature_2m[h.index]::float * 9 / 5 + 32, 1) as temperature_f,
        s.payload:hourly:relative_humidity_2m[h.index]::float as humidity_pct,
        s.payload:hourly:precipitation[h.index]::float as precipitation_mm,
        s.payload:hourly:wind_speed_10m[h.index]::float as wind_speed_kmh
    from source s,
        lateral flatten(input => s.payload:hourly:time) h

)

select *
from flattened
qualify row_number() over (
    partition by city_name, observation_ts
    order by loaded_at desc
) = 1