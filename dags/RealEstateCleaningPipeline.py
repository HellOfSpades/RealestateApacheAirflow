from pathlib import Path

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task
from pendulum import datetime


@dag(start_date=datetime(2026, 1, 1),
     schedule="@daily",
     tags=["activity", "custom dag"],
     catchup=False
     )
def clean_real_estate_pipeline():
    @task
    def write_to_csv(df, dir):
        df.to_csv(dir, index=False)
    @task
    def get_root_dir():
        return Path(__file__).resolve().parents[1]
    @task
    def load_csv(path):
        return pd.read_csv(path, dtype=str)


    BASE_DIR = get_root_dir()

    main_path = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
    output_path = BASE_DIR / "data/cleaned/Real_Estate_Sales.csv"

    main_df = load_csv(main_path)

    write_to_csv(main_df, output_path)


clean_real_estate_pipeline()