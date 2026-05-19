from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_pizza_pulse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pizza"],
):
    BashOperator(
        task_id="hello",
        bash_command="echo 'hello pizza pulse' && date",
    )