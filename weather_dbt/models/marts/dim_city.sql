select distinct
    md5(city) as city_key,
    city,
    country,
    latitude,
    longitude
from {{ ref('stg_weather_hourly') }}