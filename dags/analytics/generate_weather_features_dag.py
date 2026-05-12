from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    mart_dataset,
    weather_features_dataset,
)

from etl.analytics.create_weather_features_analytics import generate_weather_features

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}



@dag(
    dag_id="load_weather_features",
    description="Analytics DAG for weather features, MART -> FEATURES",
    default_args=DEFAULT_ARGS,
    schedule=[mart_dataset],  # dispara qdo mart atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    tags=["analytics", "weather_features"],
)


def features_analytics():

    ensure_analytics_tables = SQLExecuteQueryOperator(
        task_id="ensure_analytics_tables",
        conn_id="postgres_default",
        sql="analytics/050_create_weather_features.sql",
    )

    @task(outlets=[weather_features_dataset])
    def analytics_task():
        return generate_weather_features(postgres_conn_id="postgres_default")


    analytics = analytics_task()

    

    ensure_analytics_tables >> analytics


dag_instance = features_analytics()
