from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import staging_dataset, mart_dataset
from etl.mart.load_open_meteo_forecast_mart import load_mart


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="load_open_meteo_mart",
    description="Transform STAGING → MART",
    default_args=DEFAULT_ARGS,
    schedule=[staging_dataset],  # dispara qdo staging atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["transform", "mart"],
)


def open_meteo_mart():

    ensure_mart_tables = SQLExecuteQueryOperator(
        task_id="ensure_mart_tables",
        conn_id="postgres_default",
        sql="mart/030_create_mart_open_meteo_forecast.sql",
    )

    @task(outlets=[mart_dataset])
    def mart_task():
        return load_mart(postgres_conn_id="postgres_default")


    mart = mart_task()

    

    ensure_mart_tables >> mart

dag_instance = open_meteo_mart()
