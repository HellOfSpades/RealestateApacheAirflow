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
def clean_affordable_housing_pipeline():

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
    def year_to_jan_first(main_path, output_path, year_column_name, new_year_column_name="Date Recorded"):
        df = read_csv(main_path)
        df[year_column_name] = pd.to_datetime(df[year_column_name].astype(str) + "-01-01", format="%Y-%m-%d")
        df[year_column_name] = df[year_column_name].dt.strftime("%Y-%m-%d")
        df = df.rename(columns={year_column_name: new_year_column_name})
        write_to_csv(df, output_path)
        return output_path
    @task
    def remove_columns(main_path, output_path, column_names):
        df = read_csv(main_path)
        existing_cols = [col for col in column_names if col in df.columns]
        if existing_cols:
            df = df.drop(columns=column_names)
        write_to_csv(df, output_path)
        return output_path


    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = str(BASE_DIR / "data/raw/Affordable_Housing_by_Town_Raw.csv")
    output_path = str(BASE_DIR / "data/cleaned/Affordable_Housing.csv")

    #staging paths
    staging_path = str(BASE_DIR / "data/staging/Affordable_Housing.csv")

    staging_path = remove_columns(main_path, staging_path, ["Town Code","Government Assisted","Tenant Rental Assistance", "Single Family CHFA/ USDA Mortgages","Deed Restricted Units", "Total Assisted Units"])
    output_path = year_to_jan_first(staging_path, output_path, "Year")

clean_affordable_housing_pipeline()