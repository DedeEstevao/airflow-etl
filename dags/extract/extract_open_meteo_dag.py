
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

from etl.extract.extract_open_meteo_to_raw import extract_open_meteo
from etl.extract.extract_open_meteo_to_raw import load_open_meteo

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="extract_open_meteo_to_raw",
    description="Extract hourly weather data from Open-Meteo API into raw JSONB layer",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["extract", "api", "raw", "open-meteo"],
)
def extract_open_meteo_dag():
    @task
    def extract_and_load_task():
        payload = extract_open_meteo(
            latitude=-23.55,
            longitude=-46.63,
        )

        load_open_meteo(
            latitude=-23.55,
            longitude=-46.63,
            payload=payload,
            postgres_conn_id="postgres_default",
        )

    extract_and_load_task()


extract_open_meteo_dag()
