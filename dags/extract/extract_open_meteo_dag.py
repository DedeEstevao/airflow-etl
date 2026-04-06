
from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from etl.extract.extract_open_meteo_to_raw import extract_open_meteo
from etl.extract.extract_open_meteo_to_raw import load_open_meteo

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

raw_dataset = Dataset("postgres://postgres/airflow/raw/open_meteo_forecast")

CITIES = [
    {"name": "Sao_Paulo", "lat": -23.55, "lon": -46.63},
    {"name": "Rio", "lat": -22.90, "lon": -43.20},
    {"name": "Curitiba", "lat": -25.43, "lon": -49.27},
]


@dag(
    dag_id="extract_open_meteo_to_raw",
    description="Extract hourly weather data from Open-Meteo API into raw JSONB layer",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    tags=["extract", "api", "raw", "open-meteo"],
)
def extract_open_meteo_dag():

    @task(outlets=[raw_dataset])
    def extract_and_load_task(city: dict):
        from airflow.operators.python import get_current_context

        context = get_current_context()
        run_id = context["run_id"]

        payload = extract_open_meteo(
            latitude=city["lat"],
            longitude=city["lon"],
        )

        load_open_meteo(
            city=city["name"],
            latitude=city["lat"],
            longitude=city["lon"],
            payload=payload,
            postgres_conn_id="postgres_default",
            dag_run_id=run_id,
        )

    extract_and_load_task.expand(city=CITIES)


dag_instance = extract_open_meteo_dag()

