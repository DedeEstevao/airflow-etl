from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("🚀 Airflow funcionando!")

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["exemplo"]
) as dag:

    task_hello = PythonOperator(
        task_id="print_hello",
        python_callable=hello
    )

