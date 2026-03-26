"""
transform_property_type.py

Transformation script used by the Airflow DAG.
Reads a CSV with 'Property Type' and 'Residential Type' columns,
replaces 'Residential' values in 'Property Type' with the corresponding
'Residential Type' value, then drops the 'Residential Type' column.
"""

import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


def transform_property_type(input_path: str, output_path: str) -> None:
    """
    Load the CSV, apply the residential type replacement, and save the result.

    Rules:
      - 'Residential Type' is non-null only when 'Property Type' == 'Residential'.
      - For every row where 'Property Type' == 'Residential', replace that value
        with the corresponding 'Residential Type' value.
      - Drop the 'Residential Type' column.
      - Write the result to output_path.

    Args:
        input_path:  Absolute path to the source CSV file.
        output_path: Absolute path where the transformed CSV will be written.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info("Reading CSV from %s", input_path)
    df = pd.read_csv(input_path)

    # ── Validate required columns ──────────────────────────────────────────────
    required_columns = {"Property Type", "Residential Type"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required column(s): {missing}")

    logger.info("Loaded %d rows", len(df))

    # ── Apply transformation ───────────────────────────────────────────────────
    residential_mask = df["Property Type"] == "Residential"
    replaced_count = residential_mask.sum()

    df.loc[residential_mask, "Property Type"] = df.loc[
        residential_mask, "Residential Type"
    ]

    logger.info("Replaced %d 'Residential' entries with their Residential Type", replaced_count)

    # ── Drop the now-redundant column ─────────────────────────────────────────
    df.drop(columns=["Residential Type"], inplace=True)

    # ── Write output ───────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Transformed file written to %s", output_path)


if __name__ == "__main__":
    # Allow quick local testing:
    #   python transform_property_type.py
    import sys

    _input  = sys.argv[1] if len(sys.argv) > 1 else "data/input.csv"
    _output = sys.argv[2] if len(sys.argv) > 2 else "data/output.csv"
    logging.basicConfig(level=logging.INFO)
    transform_property_type(_input, _output)