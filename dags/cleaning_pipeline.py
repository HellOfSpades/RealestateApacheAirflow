from pathlib import Path

import pandas as pd

from scripts.ExtractColumnToExternalCsv import create_lookup_and_replace_ids


BASE_DIR = Path(__file__).resolve().parents[1]

main_path = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
ref_path = BASE_DIR / "data/cleaned/Address.csv"

# Load main dataset
main_df = pd.read_csv(main_path, dtype=str)

# Create Address.csv if it doesn't exist
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