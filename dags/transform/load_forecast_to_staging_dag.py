from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    raw_forecast_dataset,
    staging_forecast_dataset,
)
from etl.load.load_weather_forecast_staging import load_staging
from etl.quality.check_weather_forecast_staging import (
    check_staging_quality_incremental)



DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="load_forecast_weather_to_staging",
    description="Transform RAW → STAGING",
    default_args=DEFAULT_ARGS,
    schedule=[raw_forecast_dataset],  # dispara qdo raw atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["transform", "staging"],
)


def load_forecast_weather_dag():

    ensure_forecast_staging_tables = SQLExecuteQueryOperator(
        task_id="ensure_forecast_staging_tables",
        conn_id="open_meteo",
        sql="staging/020_create_staging_weather_forecast.sql",
    )

    @task(outlets=[staging_forecast_dataset])
    def staging_task():
        return load_staging(postgres_conn_id="open_meteo")

    @task
    def data_quality_task():
        from airflow.operators.python import get_current_context
        context = get_current_context()

        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]

        check_staging_quality_incremental(
            postgres_conn_id="open_meteo",
            interval_start=interval_start,
            interval_end=interval_end,
        )

    staging = staging_task()
    dq = data_quality_task()
    

    ensure_forecast_staging_tables >> staging >> dq

dag_instance = load_forecast_weather_dag()
