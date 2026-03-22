from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task
from pendulum import datetime


@dag(start_date=datetime(2020, 12, 31),
     schedule="@daily",
     tags=["activity", "custom dag"],
     catchup=False
     )
def test_dag():
    @task
    def task1():
        return "Task Return Value"

    task1()

test_dag()