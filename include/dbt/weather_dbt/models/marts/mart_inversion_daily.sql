select
    valley_city,
    date_key,
    count(*) as hours_observed,
    count_if(is_inversion) as inversion_hours,
    count_if(is_stagnant_inversion) as stagnant_inversion_hours,
    round(avg(lapse_rate_f_per_1000ft), 2) as avg_lapse_rate_f_per_1000ft,
    -- threshold is a judgment call: 6 or more inverted hours counts as an inversion day
    count_if(is_inversion) >= 6 as is_inversion_day
from {{ ref('mart_inversion_hourly') }}
group by 1, 2