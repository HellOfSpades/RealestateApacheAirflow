from pathlib import Path

from airflow import DAG
from datetime import datetime
import pandas as pd
import os

from airflow.providers.standard.operators.python import PythonOperator

BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_CSV = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
OUTPUT_CSV = BASE_DIR / "data/cleaned/Address.csv"


def transform_property_type_csv(input_csv: str, output_csv: str) -> None:
    df = pd.read_csv(input_csv)

    required_columns = ["Property Type", "Residential Type"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    mask = (
        df["Property Type"].eq("Residential")
        & df["Residential Type"].notna()
    )

    df.loc[mask, "Property Type"] = df.loc[mask, "Residential Type"]

    df = df.drop(columns=["Residential Type"])

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="transform_property_type_csv_dag",
    default_args=default_args,
    description="Replace Residential property type with Residential Type values and drop Residential Type column",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "pandas", "cleanup"],
) as dag:

    transform_csv_task = PythonOperator(
        task_id="transform_property_type_csv",
        python_callable=transform_property_type_csv,
        op_kwargs={
            "input_csv": INPUT_CSV,
            "output_csv": OUTPUT_CSV,
        },
    )

    transform_csv_task