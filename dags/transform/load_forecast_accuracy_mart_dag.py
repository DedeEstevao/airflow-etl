from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    staging_forecast_dataset,
    staging_observed_dataset, 
)

from etl.mart.load_forecast_accuracy_mart import (
    load_forecast_accuracy_mart,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="load_forecast_accuracy_mart_dag",
    description="Transform STAGING → MART",
    default_args=DEFAULT_ARGS,
    schedule=[staging_observed_dataset, 
              staging_forecast_dataset],
              # dispara qdo staging observed e forecast atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["transform", "mart"],
)


def forecast_accuracy_mart():

    ensure_mart_tables = SQLExecuteQueryOperator(
        task_id="ensure_mart_tables",
        conn_id="open_meteo",
        sql="mart/031_create_weather_forecast_accuracy.sql",
    )

    @task
    def accuracy_mart_task():
        return load_forecast_accuracy_mart(postgres_conn_id="open_meteo")


    mart = accuracy_mart_task()

    

    ensure_mart_tables >> mart

dag_instance = forecast_accuracy_mart()
