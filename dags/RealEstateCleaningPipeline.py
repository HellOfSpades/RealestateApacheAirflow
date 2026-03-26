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
    def get_root_dir():
        return Path(__file__).resolve().parents[1]

    @task
    def fix_date(main_path, output_path, date_column_name):
        df = pd.read_csv(main_path, dtype=str)
        df[date_column_name] = pd.to_datetime(
            df[date_column_name],
            format="%m/%d/%Y"
        ).dt.strftime("%Y-%m-%d")
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def remove_empty_entries(main_path, output_path, column_name):
        df = pd.read_csv(main_path, dtype=str)
        df = df[df[column_name].notna() & (df[column_name] != "")]
        df.to_csv(output_path, index=False)
        return output_path

    # fix dates where the year is 0023 or 0024 to be 2023 and 2025
    @task
    def correct_wrong_years(main_path, output_path, date_column_name):
        df = pd.read_csv(main_path, dtype=str)
        mask = df[date_column_name].str[6:8] == "00"
        df.loc[mask, date_column_name] = (
                df.loc[mask, date_column_name].str[:6] + "2" + df.loc[mask, date_column_name].str[7:]
        )
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def year_to_jan_first(main_path, output_path, year_column_name):
        df = pd.read_csv(main_path, dtype=str)
        df[year_column_name] = pd.to_datetime(df[year_column_name].astype(str) + "-01-01", format="%Y-%m-%d")
        df[year_column_name] = df[year_column_name].dt.strftime("%Y-%m-%d")
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def rename_column(main_path, output_path, old_column_name, new_column_name):
        df = pd.read_csv(main_path, dtype=str)
        df = df.rename(columns={old_column_name: new_column_name})
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def merge_columns(main_path, df_cleaned, original_cols, cleaned_cols):
        df_main = pd.read_csv(main_path, dtype=str)
        for orig_col, clean_col in zip(original_cols, cleaned_cols):
            df_main[orig_col] = df_cleaned[clean_col]
        return df_main

    @task
    def remove_column(main_path, output_path, column_name):
        df = pd.read_csv(main_path, dtype=str)
        if column_name in df.columns:
            df = df.drop(columns=[column_name])
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def update_property_type(main_path, output_path, property_col="Property Type", residential_col="Residential Type"):
        df = pd.read_csv(main_path, dtype=str)
        mask = df[property_col] == "Residential"
        df.loc[mask, property_col] = df.loc[mask, residential_col]
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def fill_missing_location(main_path, output_path,
                              location_col="Location",
                              town_col="Town",
                              address_col="Address"):
        df = pd.read_csv(main_path, dtype=str)
        # Step 1: create lookup of valid locations
        lookup = (
            df[df[location_col].notna() & (df[location_col] != "")]
            [[town_col, address_col, location_col]]
            .drop_duplicates(subset=[town_col, address_col], keep="first")
            .rename(columns={location_col: "Location_lookup"})
        )

        # Step 2: merge back
        df = df.merge(
            lookup,
            on=[town_col, address_col],
            how="left"
        )

        # Step 3: fill missing Location values
        mask = df[location_col].isna() | (df[location_col] == "")
        df.loc[mask, location_col] = df.loc[mask, "Location_lookup"]

        # Step 4: cleanup
        df = df.drop(columns=["Location_lookup"])

        df.to_csv(output_path, index=False)
        return output_path

    @task
    def extract_coordinates(main_path, output_path, location_col="Location", long_column = "Longitude", lat_column = "Latitude"):
        df = pd.read_csv(main_path, dtype=str)
        coords = df[location_col].str.extract(r"POINT \(([-\d\.]+) ([-\d\.]+)\)")

        df[long_column] = pd.to_numeric(coords[0], errors="coerce")
        df[lat_column] = pd.to_numeric(coords[1], errors="coerce")

        df.to_csv(output_path, index=False)
        return output_path

    @task
    def fill_coordinates_from_geojson(main_path, geojson_path, output_path,
                                      address_col="Address",
                                      town_col="Town"):
        import json

        lookup = {}

        with open(geojson_path, "r", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)

                props = data["properties"]
                geom = data["geometry"]

                number = (props.get("number") or "").strip()
                street = (props.get("street") or "").strip()
                city = (props.get("city") or "").strip()

                if not number or not street or not city:
                    continue

                lon, lat = geom["coordinates"]

                key = f"{number} {street}, {city}".upper()
                lookup[key] = (lon, lat)
        df = pd.read_csv(main_path, dtype=str)
        df["lookup_key"] = (
                df[address_col].astype(str).str.strip() + ", " +
                df[town_col].astype(str).str.strip()
        ).str.upper()

        mapped = df["lookup_key"].map(lookup)

        mask = df["Longitude"].isna() | df["Latitude"].isna()

        df.loc[mask, "Longitude"] = mapped[mask].str[0]
        df.loc[mask, "Latitude"] = mapped[mask].str[1]

        df = df.drop(columns=["lookup_key"])

        df.to_csv(output_path, index=False)
        return output_path



    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv"
    output_path = BASE_DIR / "data/cleaned/Real_Estate_Sales.csv"

    staging_path = BASE_DIR / "data/staging/Real_Estate_Sales.csv"

    geo_coordinates_lookup_path = BASE_DIR / "data/raw/statewide-addresses-state.geojson"

    # Fix Coordinates
    staging_path = fill_missing_location(main_path, staging_path)
    staging_path = extract_coordinates(staging_path, staging_path)
    staging_path = remove_column(staging_path, staging_path, "Location")
    staging_path = fill_coordinates_from_geojson(staging_path, geojson_path=str(geo_coordinates_lookup_path), output_path=staging_path)

    # Date Recorded branch
    staging_path = remove_empty_entries(staging_path, staging_path, column_name="Date Recorded")
    staging_path = correct_wrong_years(staging_path, staging_path, date_column_name="Date Recorded")
    staging_path = fix_date(staging_path, staging_path, date_column_name="Date Recorded")

    # List Year branch
    staging_path = year_to_jan_first(staging_path, staging_path, "List Year")
    staging_path = rename_column(staging_path, staging_path, "List Year", "List Date")

    #remove unneeded columns
    staging_path = remove_column(staging_path, staging_path, "Non Use Code")
    staging_path = remove_column(staging_path, staging_path, "Assessor Remarks")
    staging_path = remove_column(staging_path, staging_path, "OPM remarks")

    #Fix Property Type and Residential Type
    staging_path = update_property_type(staging_path, staging_path, "Property Type", "Residential Type")
    output_path = remove_column(staging_path, output_path, "Residential Type")




clean_real_estate_pipeline()