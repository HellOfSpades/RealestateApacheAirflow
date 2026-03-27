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
def clean_household_debt_pipeline():

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
    def filter_and_drop_column(
            input_csv_path,
            output_csv_path,
            column_name,
            required_value
    ):
        df = read_csv(input_csv_path)

        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in CSV")

        df = df[df[column_name] == required_value]

        df = df.drop(columns=[column_name])

        write_to_csv(df, output_csv_path)

        return output_csv_path

    @task
    def year_to_jan_first(main_path, output_path, year_column_name):
        df = read_csv(main_path)
        df[year_column_name] = pd.to_datetime(df[year_column_name].astype(str) + "-01-01", format="%Y-%m-%d")
        df[year_column_name] = df[year_column_name].dt.strftime("%Y-%m-%d")
        write_to_csv(df, output_path)
        return output_path


    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = str(BASE_DIR / "data/raw/household_debt_by_state_Raw.csv")
    output_path = str(BASE_DIR / "data/cleaned/household_debt_by_state.csv")

    #staging paths
    staging_path = str(BASE_DIR / "data/staging/household_debt_by_state.csv")

    staging_path = filter_and_drop_column(main_path, staging_path, "state_fips", "09")
    output_path = year_to_jan_first(staging_path, output_path, "year")




clean_household_debt_pipeline()