select
    city,
    country,
    ts as observed_at,
    temperature_2m as temperature_c,
    precipitation as precipitation_mm,
    relative_humidity_2m as humidity_pct,
    wind_speed_10m as wind_speed_kmh,
    latitude,
    longitude
from {{ source('raw', 'weather_hourly') }}