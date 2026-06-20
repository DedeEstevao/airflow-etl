from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.timezone import datetime

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

with (DAG(
    dag_id="infra_postgres_setup",
    description="Criação de schemas e tabelas base no Postgres",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # roda sob demanda
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],

    tags=["infra", "postgres"],
) as dag):

    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id="open_meteo",
        sql="schemas/001_create_schemas.sql",
    )

    create_raw_tables_forecast = SQLExecuteQueryOperator(
        task_id="create_raw_tables_forecast",
        conn_id="open_meteo",
        sql="raw/010_create_raw_weather_forecast.sql",
    )

    create_raw_tables_observed = SQLExecuteQueryOperator(
        task_id="create_raw_tables_observed",
        conn_id="open_meteo",
        sql="raw/011_create_raw_weather_observed.sql",
    )

    create_staging_tables_forecast = SQLExecuteQueryOperator(
        task_id="create_staging_tables_forecast",
        conn_id="open_meteo",
        sql="staging/020_create_staging_weather_forecast.sql",
    )

    create_staging_tables_observed = SQLExecuteQueryOperator(
        task_id="create_staging_tables_observed",
        conn_id="open_meteo",
        sql="staging/021_create_staging_weather_observed.sql",
    )

    create_mart_tables = SQLExecuteQueryOperator(
        task_id="create_mart_tables",
        conn_id="open_meteo",
        sql="mart/030_create_mart_weather_forecast.sql",
    )

    create_mart_accuracy_tables = SQLExecuteQueryOperator(
        task_id="create_mart_accuracy_tables",
        conn_id="open_meteo",
        sql="mart/031_create_weather_forecast_accuracy.sql",
    )

    create_analytics_weather_daily_tables = SQLExecuteQueryOperator(
        task_id="create_analytics_weather_daily_tables",
        conn_id="open_meteo",
        sql="analytics/040_create_analytics_weather_daily.sql",
    )

    (
        create_schemas 
        >> create_raw_tables_forecast 
        >> create_raw_tables_observed 
        >> create_staging_tables_forecast 
        >> create_staging_tables_observed 
        >> create_mart_tables
        >> create_mart_accuracy_tables
    )
