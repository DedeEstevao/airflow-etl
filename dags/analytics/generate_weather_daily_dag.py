from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    mart_dataset,
    weather_daily_dataset,
)

from etl.analytics.load_open_meteo_forecast_analytics import load_analytics


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="open_meteo_analytics",
    description="Analytics DAG for daily weather features, MART -> ANALYTICS",
    default_args=DEFAULT_ARGS,
    schedule=[mart_dataset],  # dispara qdo mart atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["analytics", "weather_daily"],
)


def open_meteo_analytics():

    ensure_analytics_tables = SQLExecuteQueryOperator(
        task_id="ensure_analytics_tables",
        conn_id="postgres_default",
        sql="analytics/040_create_analytics_weather_daily.sql",
    )

    @task(outlets=[weather_daily_dataset])
    def analytics_task():
        return load_analytics(postgres_conn_id="postgres_default")


    analytics = analytics_task()

    

    ensure_analytics_tables >> analytics


dag_instance = open_meteo_analytics()
