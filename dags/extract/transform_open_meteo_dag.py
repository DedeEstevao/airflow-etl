from airflow.decorators import dag, task
from airflow import Dataset
from datetime import timedelta
import pendulum

from etl.load.load_open_meteo_forecast_staging import load_staging
from etl.quality.check_open_meteo_staging import check_staging_quality_incremental
from etl.mart.load_open_meteo_forecast_mart import load_mart
from etl.analytics.load_open_meteo_forecast_analytics import load_analytics


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

raw_dataset = Dataset("postgres://postgres/airflow/raw/open_meteo_forecast")


@dag(
    dag_id="open_meteo_transform",
    description="Transform RAW → STAGING → MART",
    default_args=DEFAULT_ARGS,
    schedule=[raw_dataset],  # 🔥 dispara quando RAW atualiza
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    catchup=False,
    tags=["transform", "staging", "mart", "analytics"],
)


def open_meteo_transform():

    @task
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

    @task
    def mart_task():
        import logging
        logger = logging.getLogger(__name__)
        logger.info("🚀 Executando MART")

        load_mart(postgres_conn_id="postgres_default")

    @task
    def analytics_task():
        load_analytics(postgres_conn_id="postgres_default")

    staging = staging_task()
    dq = data_quality_task()
    mart = mart_task()
    analytics = analytics_task()


    staging >> dq >> mart >> analytics



dag_instance = open_meteo_transform()