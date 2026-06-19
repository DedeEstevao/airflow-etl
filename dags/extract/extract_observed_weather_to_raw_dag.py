
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.extract.extract_observed_to_raw import extract_observed_weather
from etl.extract.extract_observed_to_raw import load_observed_weather

from etl.common.datasets import (
    raw_observed_dataset,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


CITIES = [
    {"name": "Sao_Paulo", "lat": -23.55, "lon": -46.63},
    {"name": "Rio", "lat": -22.90, "lon": -43.20},
    {"name": "Curitiba", "lat": -25.43, "lon": -49.27},
]


@dag(
    dag_id="extract_observed_weather_to_raw",
    description="Extract hourly weather observed data from Open-Meteo API into raw"
                " JSONB layer",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],

    tags=["extract", "api", "raw", "open-meteo", "observed"],
)

    

def extract_observed_weather_dag():
    
    ensure_raw_tables = SQLExecuteQueryOperator(
        task_id="ensure_raw_weather_observed_tables",
        conn_id="open_meteo",
        sql="raw/011_create_raw_weather_observed.sql",
    )
    

    @task(outlets=[raw_observed_dataset])
    def extract_observed_city(city: dict):
        from airflow.operators.python import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        
        today = datetime.now(
            timezone.utc
            ).date()
        yesterday = (
            today - timedelta(days=1)
        )

        payload = extract_observed_weather(

        latitude=city["lat"],

        longitude=city["lon"],

        start_date=yesterday,

        end_date=yesterday,
        )
        
        load_observed_weather(
            city=city["name"],
            latitude=city["lat"],
            longitude=city["lon"],
            payload=payload,
            postgres_conn_id="open_meteo",
            dag_run_id=run_id,
        )

    extraction = extract_observed_city.expand(city=CITIES)

    ensure_raw_tables >> extraction

dag_instance = extract_observed_weather_dag()
