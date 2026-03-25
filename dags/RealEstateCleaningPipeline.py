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

    @task
    def fix_date(df, date_column_name):
        df[date_column_name] = pd.to_datetime(
            df[date_column_name],
            format="%m/%d/%Y"
        ).dt.strftime("%Y-%m-%d")
        return df

    @task
    def remove_empty_entries(df, column_name):
        df = df[df[column_name].notna() & (df[column_name] != "")]
        return df

    # fix dates where the year is 0023 or 0024 to be 2023 and 2025
    @task
    def correct_wrong_years(df, date_column_name):
        mask = df[date_column_name].str[6:8] == "00"
        df.loc[mask, date_column_name] = (
                df.loc[mask, date_column_name].str[:6] + "2" + df.loc[mask, date_column_name].str[7:]
        )
        return df

    @task
    def year_to_jan_first(df, year_column_name):
        df[year_column_name] = pd.to_datetime(df[year_column_name].astype(str) + "-01-01", format="%Y-%m-%d")
        df[year_column_name] = df[year_column_name].dt.strftime("%Y-%m-%d")
        return df

    @task
    def rename_column(df, old_column_name, new_column_name):
        df = df.rename(columns={old_column_name: new_column_name})
        return df

    @task
    def merge_columns(df_main, df_cleaned, original_cols, cleaned_cols):
        for orig_col, clean_col in zip(original_cols, cleaned_cols):
            df_main[orig_col] = df_cleaned[clean_col]
        return df_main

    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
    output_path = BASE_DIR / "data/cleaned/Real_Estate_Sales.csv"

    main_df = load_csv(str(main_path))

    # #fix Date Recorded
    # main_df = remove_empty_entries(main_df, column_name="Date Recorded")
    # main_df = correct_wrong_years(main_df, date_column_name="Date Recorded")
    # main_df = fix_date(main_df, date_column_name="Date Recorded")
    #
    # #fix List Year
    # main_df = year_to_jan_first(main_df, "List Year")
    # main_df = rename_column(main_df, "List Year", "List Date")

    # Date Recorded branch
    date_df = remove_empty_entries(main_df, column_name="Date Recorded")
    date_df = correct_wrong_years(date_df, date_column_name="Date Recorded")
    date_df = fix_date(date_df, date_column_name="Date Recorded")

    # List Year branch
    list_df = year_to_jan_first(main_df, "List Year")
    list_df = rename_column(list_df, "List Year", "List Date")

    # Merge cleaned columns back
    merged_df = merge_columns(main_df, date_df, ["Date Recorded"], ["Date Recorded"])
    merged_df = merge_columns(merged_df, list_df, ["List Year"], ["List Date"])


    write_to_csv(merged_df, str(output_path))


clean_real_estate_pipeline()