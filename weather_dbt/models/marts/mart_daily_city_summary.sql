select
    c.city,
    c.country,
    d.date_key as date,
    d.month_name,
    d.is_weekend,
    round(avg(f.temperature_c), 1) as avg_temp_c,
    max(f.temperature_c) as max_temp_c,
    min(f.temperature_c) as min_temp_c,
    round(sum(f.precipitation_mm), 1) as total_precip_mm,
    round(avg(f.humidity_pct), 1) as avg_humidity_pct,
    round(avg(f.wind_speed_kmh), 1) as avg_wind_kmh
from {{ ref('fact_weather_hourly') }} f
join {{ ref('dim_city') }} c on f.city_key = c.city_key
join {{ ref('dim_date') }} d on f.date_key = d.date_key
group by c.city, c.country, d.date_key, d.month_name, d.is_weekend