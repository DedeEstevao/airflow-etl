from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import json
from datetime import timedelta

DEFAULT_ARGS = ({
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
})


@dag(
    dag_id="extract_open_meteo_to_raw",
    description="Extract hourly weather data from Open-Meteo API into raw JSONB layer",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["extract", "api", "raw", "open-meteo"],
)
def extract_open_meteo():
    @task
    def extract_and_load():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=-23.55"
            "&longitude=-46.63"
            "&hourly=temperature_2m,precipitation"
        )
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        payload = response.json()
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        insert_sql = """
            INSERT INTO raw.open_meteo_forecast
            (latitude, longitude, payload)
            VALUES (%s, %s, %s)
        """
        pg_hook.run(
            insert_sql,
            parameters=(
                -23.55,
                -46.63,
                json.dumps(payload),
            ),
        )
    extract_and_load()


extract_open_meteo()


