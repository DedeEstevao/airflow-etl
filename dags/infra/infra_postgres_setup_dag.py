from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.timezone import datetime

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="infra_postgres_setup",
    description="Criação de schemas e tabelas base no Postgres",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # roda sob demanda
    catchup=False,
    template_searchpath=[
        "/opt/airflow/dags",
        "/opt/airflow/sql",
    ],

    tags=["infra", "postgres"],
) as dag:

    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id="postgres_default",
        sql="schemas/001_create_schemas.sql",
    )

    create_raw_tables = SQLExecuteQueryOperator(
        task_id="create_raw_tables",
        conn_id="postgres_default",
        sql="raw/010_create_raw_orders.sql",
    )

    create_staging_tables = SQLExecuteQueryOperator(
        task_id="create_staging_tables",
        conn_id="postgres_default",
        sql="staging/020_create_staging_orders.sql",
    )

    create_analytics_tables = SQLExecuteQueryOperator(
        task_id="create_analytics_tables",
        conn_id="postgres_default",
        sql="analytics/030_create_fact_sales.sql",
    )

    create_schemas >> create_raw_tables >> create_staging_tables >> create_analytics_tables
