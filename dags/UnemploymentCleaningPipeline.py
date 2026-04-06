import os
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
def clean_unemployment_pipeline():

    def write_to_csv(df, file_path):
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, index=False)

    def read_csv(file_path, dtype=str):
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file does not exist: {file_path}")

        return pd.read_csv(file_path, dtype=dtype)

    @task
    def fix_date(main_path, output_path, date_column_name):
        df = read_csv(main_path)
        df = df.rename(columns={date_column_name: "Date Recorded"})
        write_to_csv(df, output_path)
        return output_path

    @task
    def write_clean_csv(input_path, output_path):
        df = read_csv(input_path)
        write_to_csv(df, output_path)
        try:
            if os.path.exists(input_path):
                os.remove(input_path)
        except Exception as e:
            print(f"Failed to delete {input_path}: {e}")
        return output_path

    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = str(BASE_DIR / "data/raw/UNEMPLOYCT_Raw.csv")
    output_path = str(BASE_DIR / "data/cleaned/Unemployement.csv")

    #staging paths
    staging_path = str(BASE_DIR / "data/staging/Unemployment.csv")

    staging_path = fix_date(main_path, staging_path, "observation_date")
    output_path = write_clean_csv(staging_path, output_path)

clean_unemployment_pipeline()