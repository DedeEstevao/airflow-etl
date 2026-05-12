from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    weather_features_dataset,
    model_metadata_dataset,
)

from etl.ml.train_model import (
    run_training,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="train_weather_model",
    description=(
        "Train ML model using "
        "weather features"
    ),
    default_args=DEFAULT_ARGS,
    schedule=[weather_features_dataset],
    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    max_active_runs=1,
    tags=["ml", "training"],
)
def train_weather_model_dag():

    ensure_analytics_tables = SQLExecuteQueryOperator(
        task_id="ensure_ml_tables",
        conn_id="postgres_default",
        sql="analytics/060_create_model_metadata.sql",
    )
    @task(outlets=[model_metadata_dataset])
    def train_task():

        return run_training(
            postgres_conn_id="postgres_default"
        )

    analytics = train_task()

    ensure_analytics_tables >> analytics

dag_instance = train_weather_model_dag()
