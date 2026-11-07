import time
from datetime import date, timedelta

import pandas as pd
import requests

CITIES = [
    ("Lahore", "Pakistan", 31.5204, 74.3587),
    ("Karachi", "Pakistan", 24.8607, 67.0011),
    ("Islamabad", "Pakistan", 33.6844, 73.0479),
    ("Peshawar", "Pakistan", 34.0151, 71.5249),
    ("Quetta", "Pakistan", 30.1798, 66.9750),
    ("Multan", "Pakistan", 30.1575, 71.5249),
    ("Faisalabad", "Pakistan", 31.4504, 73.1350),
    ("Rawalpindi", "Pakistan", 33.5651, 73.0169),
    ("Hyderabad", "Pakistan", 25.3960, 68.3578),
    ("Sialkot", "Pakistan", 32.4945, 74.5229),
    ("Dubai", "UAE", 25.2048, 55.2708),
    ("London", "UK", 51.5072, -0.1276),
    ("New York", "USA", 40.7128, -74.0060),
    ("Tokyo", "Japan", 35.6762, 139.6503),
    ("Sydney", "Australia", -33.8688, 151.2093),
    ("Delhi", "India", 28.6139, 77.2090),
    ("Istanbul", "Turkey", 41.0082, 28.9784),
    ("Toronto", "Canada", 43.6532, -79.3832),
    ("Singapore", "Singapore", 1.3521, 103.8198),
    ("Cairo", "Egypt", 30.0444, 31.2357),
]

URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = "temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m"


def extract_all_cities(out_path="weather_hourly.csv"):
    end = date.today() - timedelta(days=7)
    start = end - timedelta(days=364)
    frames = []
    for city, country, lat, lon in CITIES:
        for attempt in range(3):
            r = requests.get(URL, params={
                "latitude": lat, "longitude": lon,
                "start_date": start.isoformat(), "end_date": end.isoformat(),
                "hourly": HOURLY, "timezone": "GMT",
            }, timeout=60)
            if r.status_code == 200:
                break
            time.sleep(10)
        r.raise_for_status()
        df = pd.DataFrame(r.json()["hourly"]).rename(columns={"time": "ts"})
        df.insert(0, "city", city)
        df.insert(1, "country", country)
        df["latitude"] = lat
        df["longitude"] = lon
        frames.append(df)
        print(city, len(df))
        time.sleep(1)
    out = pd.concat(frames, ignore_index=True)
    out.to_csv(out_path, index=False)
    print("rows:", len(out))
    return out_path


if __name__ == "__main__":
    extract_all_cities()