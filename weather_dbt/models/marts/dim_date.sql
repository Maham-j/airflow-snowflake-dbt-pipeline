with bounds as (
    select
        min(observed_at)::date as start_date,
        max(observed_at)::date as end_date
    from {{ ref('stg_weather_hourly') }}
),

spine as (
    select
        dateadd(
            day,
            row_number() over (order by seq4()) - 1,
            (select start_date from bounds)
        ) as date_day
    from table(generator(rowcount => 1000))
)

select
    date_day as date_key,
    year(date_day) as year,
    month(date_day) as month,
    monthname(date_day) as month_name,
    day(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week,
    dayname(date_day) as day_name,
    dayofweekiso(date_day) in (6, 7) as is_weekend
from spine
where date_day <= (select end_date from bounds)