import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

CSV_PATH = "/opt/airflow/dags/weather_hourly.csv"


def extract():
    from weather_extract import extract_all_cities
    extract_all_cities(CSV_PATH)


def load():
    import snowflake.connector

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database="WEATHER_DB",
        schema="RAW",
        warehouse="WEATHER_WH",
    )
    cur = conn.cursor()
    try:
        cur.execute('''CREATE TABLE IF NOT EXISTS WEATHER_HOURLY (
            city STRING, country STRING, ts TIMESTAMP_NTZ,
            temperature_2m FLOAT, precipitation FLOAT,
            relative_humidity_2m FLOAT, wind_speed_10m FLOAT,
            latitude FLOAT, longitude FLOAT)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS WEATHER_HOURLY_STG (
            city STRING, country STRING, ts STRING,
            temperature_2m FLOAT, precipitation FLOAT,
            relative_humidity_2m FLOAT, wind_speed_10m FLOAT,
            latitude FLOAT, longitude FLOAT)''')
        cur.execute("TRUNCATE TABLE WEATHER_HOURLY_STG")
        cur.execute(f"PUT file://{CSV_PATH} @%WEATHER_HOURLY_STG OVERWRITE=TRUE")
        cur.execute('''COPY INTO WEATHER_HOURLY_STG
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            PURGE = TRUE''')
        cur.execute('''MERGE INTO WEATHER_HOURLY t
            USING (
                SELECT city, country,
                       TO_TIMESTAMP_NTZ(ts, 'YYYY-MM-DD"T"HH24:MI') AS ts,
                       temperature_2m, precipitation, relative_humidity_2m,
                       wind_speed_10m, latitude, longitude
                FROM WEATHER_HOURLY_STG
            ) s
            ON t.city = s.city AND t.ts = s.ts
            WHEN MATCHED THEN UPDATE SET
                t.country = s.country,
                t.temperature_2m = s.temperature_2m,
                t.precipitation = s.precipitation,
                t.relative_humidity_2m = s.relative_humidity_2m,
                t.wind_speed_10m = s.wind_speed_10m,
                t.latitude = s.latitude,
                t.longitude = s.longitude
            WHEN NOT MATCHED THEN INSERT (
                city, country, ts, temperature_2m, precipitation,
                relative_humidity_2m, wind_speed_10m, latitude, longitude)
            VALUES (
                s.city, s.country, s.ts, s.temperature_2m, s.precipitation,
                s.relative_humidity_2m, s.wind_speed_10m, s.latitude, s.longitude)''')
        cur.execute("SELECT COUNT(*) FROM WEATHER_HOURLY")
        print("rows in WEATHER_HOURLY:", cur.fetchone()[0])
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="weather_hourly_to_snowflake",
    description="Hourly weather for 20 cities: API -> Snowflake RAW",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract_hourly", python_callable=extract)
    load_task = PythonOperator(task_id="load_hourly", python_callable=load)
    extract_task >> load_task