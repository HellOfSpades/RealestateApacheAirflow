import datetime

import pendulum
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="process_real_estate_data",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_real_estate_data():
    from pathlib import Path
    import pandas as pd
    from scripts.ExtractColumnToExternalCsv import create_lookup_and_replace_ids

    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
    ref_path = BASE_DIR / "data/cleaned/Address.csv"

    main_df = pd.read_csv(main_path, dtype=str)

    if not ref_path.exists():
        ref_df = pd.DataFrame(columns=["Address ID", "Town Name", "Address"])
        ref_df.to_csv(ref_path, index=False)
    else:
        ref_df = pd.read_csv(ref_path)

    main_df, ref_df = create_lookup_and_replace_ids(
        main_df=main_df,
        ref_df=ref_df,
        main_columns=["Town", "Address"],
        ref_columns=["Town Name", "Address"],
        main_id_column="Address ID",
        ref_id_column="Address ID"
    )

    main_df.to_csv(BASE_DIR / "data/cleaned/Real_Estate_Sales.csv", index=False)
    ref_df.to_csv(BASE_DIR / "data/cleaned/Address.csv", index=False)

process_real_estate_data_dag = process_real_estate_data()