select *
from {{ ref('mart_inversion_hourly') }}
where lapse_rate_f_per_1000ft < -16
   or lapse_rate_f_per_1000ft > 16