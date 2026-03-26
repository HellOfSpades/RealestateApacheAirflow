from pathlib import Path

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

    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
    ref_path = BASE_DIR / "data/cleaned/Real_Estate_Sales.csv"





test_dag()