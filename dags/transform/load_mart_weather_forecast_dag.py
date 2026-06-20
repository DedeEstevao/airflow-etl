from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    staging_forecast_dataset, 
    mart_dataset,
)

from etl.mart.load_weather_forecast_mart import (
    load_mart,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="load_weather_forecast_mart",
    description="Transform STAGING → MART",
    default_args=DEFAULT_ARGS,
    schedule=[staging_forecast_dataset],  # dispara qdo staging atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["transform", "mart"],
)


def weather_forecast_mart():

    ensure_mart_tables = SQLExecuteQueryOperator(
        task_id="ensure_mart_tables",
        conn_id="open_meteo",
        sql="mart/030_create_mart_weather_forecast.sql",
    )

    @task(outlets=[mart_dataset])
    def mart_task():
        return load_mart(postgres_conn_id="open_meteo")


    mart = mart_task()

    

    ensure_mart_tables >> mart

dag_instance = weather_forecast_mart()
