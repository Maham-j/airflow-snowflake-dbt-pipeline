FROM apache/airflow:3.2.2
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir --only-binary=:all: "dbt-snowflake<2"