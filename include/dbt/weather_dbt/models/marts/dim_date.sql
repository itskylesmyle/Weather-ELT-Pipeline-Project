with spine as (

    select row_number() over (order by seq4()) - 1 as n
    from table(generator(rowcount => 2000))

),

dates as (

    select dateadd(day, n, '2025-01-01'::date) as date_day
    from spine

)

select
    date_day as date_key,
    year(date_day) as year_num,
    month(date_day) as month_num,
    day(date_day) as day_of_month,
    dayname(date_day) as day_name,
    dayofweekiso(date_day) in (6, 7) as is_weekend
from dates