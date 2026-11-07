{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['city_key', 'observed_at']
) }}

select
    md5(city) as city_key,
    observed_at::date as date_key,
    observed_at,
    temperature_c,
    precipitation_mm,
    humidity_pct,
    wind_speed_kmh
from {{ ref('stg_weather_hourly') }}

{% if is_incremental() %}
where observed_at >= (
    select dateadd(day, -3, max(observed_at)) from {{ this }}
)
{% endif %}