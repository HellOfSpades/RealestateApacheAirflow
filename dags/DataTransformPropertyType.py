"""
dag_transform_property_type.py

Airflow DAG – Property Type CSV transformation pipeline.

Pipeline steps
──────────────
1. validate_input   – Check the source file exists and has the expected columns.
2. transform        – Replace 'Residential' values with their Residential Type,
                      then drop the Residential Type column.
3. validate_output  – Sanity-check the output file (row count, column presence).

Configuration
─────────────
Set the Airflow Variables below (Admin → Variables in the UI), or override them
directly in the `DEFAULT_CONFIG` dict for local development:

  • property_type_input_path   – Full path to the source CSV file.
  • property_type_output_path  – Full path where the result CSV will be written.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from scripts.TransformPropertyType import transform_property_type

# ── Import the reusable transformation function ────────────────────────────────
# Adjust the import path to wherever transform_property_type.py lives on your
# Airflow worker(s) / PYTHONPATH.


logger = logging.getLogger(__name__)

# ── Default paths (overridden by Airflow Variables when set) ──────────────────
DEFAULT_CONFIG = {
    "input_path":  "/opt/airflow/data/input.csv",
    "output_path": "/opt/airflow/data/output.csv",
}

# ── DAG default arguments ──────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task functions ─────────────────────────────────────────────────────────────

def _get_paths() -> tuple[str, str]:
    """Resolve input/output paths from Airflow Variables with sensible defaults."""
    input_path  = Variable.get("property_type_input_path",  default_var=DEFAULT_CONFIG["input_path"])
    output_path = Variable.get("property_type_output_path", default_var=DEFAULT_CONFIG["output_path"])
    return input_path, output_path


def validate_input(**context) -> None:
    """
    Task 1 – Validate the source CSV.

    Checks:
      • The file exists and is non-empty.
      • Both required columns are present.
      • 'Residential Type' is null for every non-Residential row (data contract).
    """
    input_path, _ = _get_paths()

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"[validate_input] Source file not found: {input_path}")

    if os.path.getsize(input_path) == 0:
        raise ValueError(f"[validate_input] Source file is empty: {input_path}")

    df = pd.read_csv(input_path)

    required_columns = {"Property Type", "Residential Type"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"[validate_input] Missing column(s): {missing}")

    # Data-contract check: Residential Type should only be populated for
    # rows where Property Type is 'Residential'.
    bad_rows = df[
        df["Residential Type"].notna() & (df["Property Type"] != "Residential")
    ]
    if not bad_rows.empty:
        logger.warning(
            "[validate_input] %d row(s) have a non-null 'Residential Type' "
            "but 'Property Type' != 'Residential'. They will be left untouched "
            "by the transformation.",
            len(bad_rows),
        )

    logger.info(
        "[validate_input] OK – %d rows, %d residential rows",
        len(df),
        (df["Property Type"] == "Residential").sum(),
    )

    # Push row count for downstream validation
    context["ti"].xcom_push(key="input_row_count", value=len(df))


def run_transform(**context) -> None:
    """
    Task 2 – Execute the transformation.

    Delegates to `transform_property_type()` from the companion script.
    """
    input_path, output_path = _get_paths()

    logger.info("[run_transform] Transforming %s → %s", input_path, output_path)
    transform_property_type(input_path, output_path)
    logger.info("[run_transform] Transformation complete.")


def validate_output(**context) -> None:
    """
    Task 3 – Validate the output CSV.

    Checks:
      • The output file exists.
      • 'Residential Type' column has been removed.
      • 'Property Type' column is still present.
      • Output row count matches the input row count (no rows lost/added).
      • No remaining 'Residential' values in 'Property Type'.
    """
    _, output_path = _get_paths()

    if not os.path.exists(output_path):
        raise FileNotFoundError(f"[validate_output] Output file not found: {output_path}")

    df = pd.read_csv(output_path)

    if "Residential Type" in df.columns:
        raise ValueError("[validate_output] 'Residential Type' column was NOT removed.")

    if "Property Type" not in df.columns:
        raise ValueError("[validate_output] 'Property Type' column is missing from output.")

    # Row-count parity
    input_row_count = context["ti"].xcom_pull(
        key="input_row_count", task_ids="validate_input"
    )
    if input_row_count is not None and len(df) != input_row_count:
        raise ValueError(
            f"[validate_output] Row count mismatch – expected {input_row_count}, "
            f"got {len(df)}."
        )

    # No residual 'Residential' values should remain
    residual = (df["Property Type"] == "Residential").sum()
    if residual > 0:
        raise ValueError(
            f"[validate_output] {residual} row(s) still have 'Residential' in "
            f"'Property Type' after transformation."
        )

    logger.info(
        "[validate_output] OK – %d rows, columns: %s",
        len(df),
        list(df.columns),
    )


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="transform_property_type",
    description=(
        "Load a CSV with Property Type / Residential Type columns, "
        "replace Residential rows with the specific residential sub-type, "
        "and drop the Residential Type column."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Trigger manually or change to a cron expression
    catchup=False,
    tags=["csv", "transformation", "property"],
) as dag:

    task_validate_input = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
        provide_context=True,
    )

    task_validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
        provide_context=True,
    )

    # ── Dependency chain ───────────────────────────────────────────────────────
    task_validate_input >> task_transform >> task_validate_output