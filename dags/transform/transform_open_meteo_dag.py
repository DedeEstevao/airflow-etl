from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    raw_dataset,
    staging_dataset,
)
from etl.load.load_open_meteo_forecast_staging import load_staging
from etl.quality.check_open_meteo_staging import (
    check_staging_quality_incremental)



DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="open_meteo_transform",
    description="Transform RAW → STAGING",
    default_args=DEFAULT_ARGS,
    schedule=[raw_dataset],  # dispara qdo raw atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["transform", "staging"],
)


def open_meteo_transform():

    ensure_staging_tables = SQLExecuteQueryOperator(
        task_id="ensure_staging_tables",
        conn_id="postgres_default",
        sql="staging/020_create_staging_open_meteo_forecast.sql",
    )

    @task(outlets=[staging_dataset])
    def staging_task():
        return load_staging(postgres_conn_id="postgres_default")

    @task
    def data_quality_task():
        from airflow.operators.python import get_current_context
        context = get_current_context()

        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]

        check_staging_quality_incremental(
            postgres_conn_id="postgres_default",
            interval_start=interval_start,
            interval_end=interval_end,
        )

    staging = staging_task()
    dq = data_quality_task()
    

    ensure_staging_tables >> staging >> dq

dag_instance = open_meteo_transform()
