from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
)

from etl.common.datasets import (
    model_metadata_dataset,
    trained_model_dataset,
)

from etl.ml.predict_model import (
    run_prediction,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="predict_weather_model",
    description=(
        "Predict weather temperatures using "
        "trained ML model"
    ),
    default_args=DEFAULT_ARGS,
    schedule=[model_metadata_dataset],
    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],
    max_active_runs=1,
    tags=["ml", "prediction"],
)
def predict_weather_model_dag():

    ensure_analytics_tables = SQLExecuteQueryOperator(
        task_id="ensure_ml_tables",
        conn_id="postgres_default",
        sql="analytics/070_create_weather_predictions.sql",
    )
    @task(outlets=[trained_model_dataset])
    def predict_task():

        return run_prediction(
            postgres_conn_id="postgres_default"
        )

    analytics = predict_task()

    ensure_analytics_tables >> analytics

dag_instance = predict_weather_model_dag()
