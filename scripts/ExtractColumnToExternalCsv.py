import pandas as pd

def create_lookup_and_replace_ids(
    main_df: pd.DataFrame,
    ref_df: pd.DataFrame,
    main_columns: list,
    ref_columns: list,
    main_id_column: str,
    ref_id_column: str
):
    """
    Extract columns from main_df into a lookup table (ref_df) and replace them
    in main_df with a single ID column.

    Parameters
    ----------
    main_df : pd.DataFrame
    ref_df : pd.DataFrame
    main_columns : list
        Column names in the main dataframe
    ref_columns : list
        Corresponding column names in the reference dataframe
    main_id_column : str
        Name of the ID column in the main dataframe
    ref_id_column : str
        Name of the ID column in the reference dataframe

    Returns
    -------
    (main_df, ref_df)
    """

    if len(main_columns) != len(ref_columns):
        raise ValueError("main_columns and ref_columns must have the same length.")

    main_df = main_df.copy()
    ref_df = ref_df.copy()

    rename_map = dict(zip(main_columns, ref_columns))

    # --- Rename main columns to match reference columns temporarily
    working_main = main_df.rename(columns=rename_map)

    # Ensure reference table structure
    expected_cols = [ref_id_column] + ref_columns
    if ref_df.empty:
        ref_df = pd.DataFrame(columns=expected_cols)

    if ref_id_column in ref_df.columns:
        ref_df[ref_id_column] = pd.to_numeric(ref_df[ref_id_column], errors="coerce")
        ref_df = ref_df.dropna(subset=[ref_id_column])
        ref_df[ref_id_column] = ref_df[ref_id_column].astype(int)
    else:
        ref_df[ref_id_column] = pd.Series(dtype="int")

    # Step 1: unique combinations
    unique_main = working_main[ref_columns].drop_duplicates()

    # Step 2: merge with existing reference table
    merged = pd.merge(unique_main, ref_df, on=ref_columns, how="left")

    # Step 3: assign new IDs
    max_id = ref_df[ref_id_column].max() if not ref_df.empty else 0
    missing_mask = merged[ref_id_column].isna()
    num_missing = missing_mask.sum()

    if num_missing > 0:
        merged.loc[missing_mask, ref_id_column] = range(
            max_id + 1, max_id + 1 + num_missing
        )

    merged[ref_id_column] = merged[ref_id_column].astype(int)

    # Step 4: update reference table
    new_entries = merged[missing_mask]
    ref_df = pd.concat([ref_df, new_entries], ignore_index=True)

    # Step 5: map IDs back to main dataframe
    working_main = working_main.merge(merged, on=ref_columns, how="left")

    # Rename ID column for main dataframe
    working_main.rename(columns={ref_id_column: main_id_column}, inplace=True)

    # Step 6: drop extracted columns
    working_main.drop(columns=ref_columns, inplace=True)

    return working_main, ref_df