select city_key, observed_at, count(*) as n
from {{ ref('fact_weather_hourly') }}
group by 1, 2
having count(*) > 1