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
    def year_quarter_to_date(main_path, output_path, year_column, quarter_column, new_date_column="date"):
        df = read_csv(main_path)

        for col in [year_column, quarter_column]:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in CSV")


        df[year_column] = df[year_column].astype(int)
        df[quarter_column] = df[quarter_column].astype(int)

        quarter_to_month = {
            1: 1,
            2: 4,
            3: 7,
            4: 10
        }

        if not df[quarter_column].isin(quarter_to_month.keys()).all():
            raise ValueError("Quarter column must only contain values 1–4")

        df[new_date_column] = pd.to_datetime(
            df[year_column].astype(str) + "-" +
            df[quarter_column].map(quarter_to_month).astype(str) + "-01",
            format="%Y-%m-%d"
        ).dt.strftime("%Y-%m-%d")

        df = df.drop(columns=[year_column, quarter_column])

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

    main_path = str(BASE_DIR / "data/raw/household_debt_by_state_Raw.csv")
    output_path = str(BASE_DIR / "data/cleaned/household_debt.csv")

    #staging paths
    staging_path = str(BASE_DIR / "data/staging/household_debt.csv")

    staging_path = filter_and_drop_column(main_path, staging_path, "state_fips", "09")
    staging_path = year_quarter_to_date(staging_path, staging_path, "year", "qtr", "Date Recorded")

    output_path = write_clean_csv(staging_path, output_path)



clean_household_debt_pipeline()