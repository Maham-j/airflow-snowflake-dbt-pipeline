select *
from {{ ref('fact_weather_hourly') }}
where temperature_c < -60 or temperature_c > 60