with source as (

    select * from {{ source('raw', 'open_meteo_raw') }}

),

flattened as (

    select
        s.city_name,
        s.latitude,
        s.longitude,
        round(s.payload:elevation::float * 3.28084) as elevation_ft,
        s.data_date,
        s.loaded_at,
        h.value::timestamp_ntz as observation_ts,
        round(s.payload:hourly:temperature_2m[h.index]::float * 9 / 5 + 32, 1) as temperature_f,
        s.payload:hourly:relative_humidity_2m[h.index]::float as humidity_pct,
        round(s.payload:hourly:precipitation[h.index]::float / 25.4, 3) as precipitation_in,
        round(s.payload:hourly:wind_speed_10m[h.index]::float / 1.609344, 1) as wind_speed_mph
    from source s,
        lateral flatten(input => s.payload:hourly:time) h

)

select *
from flattened
qualify row_number() over (
    partition by city_name, observation_ts
    order by loaded_at desc
) = 1