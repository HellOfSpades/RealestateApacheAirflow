import os
from pathlib import Path

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task
from pendulum import datetime

import numpy as np
from sklearn.ensemble import RandomForestRegressor
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path


@dag(start_date=datetime(2026, 1, 1),
     schedule="@daily",
     tags=["activity", "custom dag"],
     catchup=False
     )
def clean_real_estate_pipeline():
    @task
    def get_root_dir():
        return Path(__file__).resolve().parents[1]

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
    def fix_date(main_path, output_path, date_column_name):
        df = read_csv(main_path)
        df[date_column_name] = pd.to_datetime(
            df[date_column_name],
            format="%m/%d/%Y"
        ).dt.strftime("%Y-%m-%d")
        write_to_csv(df, output_path)
        return output_path

    @task
    def remove_empty_entries(main_path, output_path, column_name):
        df = read_csv(main_path)
        df = df[df[column_name].notna() & (df[column_name] != "")]
        write_to_csv(df, output_path)
        return output_path

    # fix dates where the year is 0023 or 0024 to be 2023 and 2025
    @task
    def correct_wrong_years(main_path, output_path, date_column_name):
        df = read_csv(main_path)
        mask = df[date_column_name].str[6:8] == "00"
        df.loc[mask, date_column_name] = (
                df.loc[mask, date_column_name].str[:6] + "2" + df.loc[mask, date_column_name].str[7:]
        )
        write_to_csv(df, output_path)
        return output_path

    @task
    def year_to_jan_first(main_path, output_path, year_column_name):
        df = read_csv(main_path)
        df[year_column_name] = pd.to_datetime(df[year_column_name].astype(str) + "-01-01", format="%Y-%m-%d")
        df[year_column_name] = df[year_column_name].dt.strftime("%Y-%m-%d")
        write_to_csv(df, output_path)
        return output_path

    @task
    def rename_column(main_path, output_path, old_column_name, new_column_name):
        df = read_csv(main_path)
        df = df.rename(columns={old_column_name: new_column_name})
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

    @task
    def update_property_type(main_path, output_path, property_col="Property Type", residential_col="Residential Type"):
        df = read_csv(main_path)
        mask = df[property_col] == "Residential"
        df.loc[mask, property_col] = df.loc[mask, residential_col]
        write_to_csv(df, output_path)
        return output_path

    @task
    def fill_missing_location(main_path, output_path,
                              location_col="Location",
                              town_col="Town",
                              address_col="Address"):
        df = read_csv(main_path)
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

        write_to_csv(df, output_path)
        return output_path

    @task
    def extract_coordinates(main_path, output_path, location_col="Location", long_column = "Longitude", lat_column = "Latitude"):
        df = read_csv(main_path)
        coords = df[location_col].str.extract(r"POINT \(([-\d\.]+) ([-\d\.]+)\)")

        df[long_column] = pd.to_numeric(coords[0], errors="coerce")
        df[lat_column] = pd.to_numeric(coords[1], errors="coerce")

        write_to_csv(df, output_path)
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
        df = read_csv(main_path)
        df["lookup_key"] = (
                df[address_col].astype(str).str.strip() + ", " +
                df[town_col].astype(str).str.strip()
        ).str.upper()

        mapped = df["lookup_key"].map(lookup)

        mask = df["Longitude"].isna() | df["Latitude"].isna()

        df.loc[mask, "Longitude"] = mapped[mask].str[0]
        df.loc[mask, "Latitude"] = mapped[mask].str[1]

        df = df.drop(columns=["lookup_key"])

        write_to_csv(df, output_path)
        return output_path

    @task(multiple_outputs=True)
    def split_columns(input_path, output_path_1, output_path_2, columns_to_extract):
        df = read_csv(input_path)
        df1 = df[columns_to_extract].copy()
        df2 = df.drop(columns=columns_to_extract, errors="ignore")
        write_to_csv(df1, output_path_1)
        write_to_csv(df2, output_path_2)
        return {
            "selected": output_path_1,
            "remaining": output_path_2
        }

    @task
    def merge_files(file_paths, output_path):
        dfs = [read_csv(p) for p in file_paths]
        df_merged = pd.concat(dfs, axis=1)

        # Delete staging files before writing output
        for path in file_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                print(f"Failed to delete {path}: {e}")

        write_to_csv(df_merged, output_path)
        return output_path

    @task(multiple_outputs=True)
    def generate_ml_map_layer_with_metrics(cleaned_csv_path, geojson_output_path, metrics_csv_path):
        import pandas as pd
        import numpy as np
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.model_selection import train_test_split
        from sklearn import metrics
        import geopandas as gpd
        from shapely.geometry import Point
        from pathlib import Path

        df = pd.read_csv(cleaned_csv_path)

        df['Date Recorded'] = pd.to_datetime(df['Date Recorded'])
        df['month'] = df['Date Recorded'].dt.month

        df = df.dropna(subset=['Latitude', 'Longitude', 'Sales Ratio', 'month'])

        X = df[['Latitude', 'Longitude', 'month']].apply(pd.to_numeric)
        y = pd.to_numeric(df['Sales Ratio'])

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.20, random_state=42
        )

        model = RandomForestRegressor(n_estimators=50, max_depth=12, n_jobs=-1, random_state=42)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        mae = metrics.mean_absolute_error(y_test, y_pred)
        mse = metrics.mean_squared_error(y_test, y_pred)
        r2 = metrics.r2_score(y_test, y_pred)

        metrics_data = {
            'Metric': ['Mean Absolute Error (MAE)', 'Mean Squared Error (MSE)', 'R-squared (R2)'],
            'Value': [float(mae), float(mse), float(r2)],
            'Description': [
                'Average magnitude of the prediction errors.',
                'Average squared difference between prediction and actual values.',
                'Percentage of variance explained by the coordinates and month.'
            ]
        }
        metrics_df = pd.DataFrame(metrics_data)

        Path(metrics_csv_path).parent.mkdir(parents=True, exist_ok=True)
        metrics_df.to_csv(metrics_csv_path, index=False)
        print(f"Metrics successfully saved to {metrics_csv_path}")

        lat_min, lat_max = X['Latitude'].min(), X['Latitude'].max()
        lon_min, lon_max = X['Longitude'].min(), X['Longitude'].max()

        lat_grid = np.linspace(lat_min, lat_max, 100)
        lon_grid = np.linspace(lon_min, lon_max, 100)

        grid_points = []
        months = np.arange(1, 13)

        for lat in lat_grid:
            for lon in lon_grid:
                pred_df = pd.DataFrame({'Latitude': lat, 'Longitude': lon, 'month': months})
                predictions = model.predict(pred_df)

                grid_points.append({
                    'Latitude': lat,
                    'Longitude': lon,
                    'sales_ratio': float(np.mean(predictions)),
                    'buy_month': int(months[np.argmin(predictions)]),
                    'sell_month': int(months[np.argmax(predictions)])
                })

        grid_df = pd.DataFrame(grid_points)
        geometry = [Point(xy) for xy in zip(grid_df['Longitude'], grid_df['Latitude'])]
        gdf = gpd.GeoDataFrame(grid_df, geometry=geometry, crs="EPSG:4326")

        Path(geojson_output_path).parent.mkdir(parents=True, exist_ok=True)
        gdf.to_file(geojson_output_path, driver="GeoJSON")

        return {
            "geojson_layer": geojson_output_path,
            "metrics_csv": metrics_csv_path
        }


    BASE_DIR = Path(__file__).resolve().parents[1]

    main_path = str(BASE_DIR / "data/raw/Real_Estate_Sales_Raw.csv")
    output_path = str(BASE_DIR / "data/cleaned/Real_Estate_Sales.csv")

    geojson_layer_path = str(BASE_DIR / "data/output/map_layer.geojson")

    #staging paths
    staging_path = str(BASE_DIR / "data/staging/Real_Estate_Sales.csv")
    staging_path_coordinates = str(BASE_DIR / "data/staging/Real_Estate_Sales_Coordinates.csv")
    staging_path_date_recorded = str(BASE_DIR / "data/staging/Real_Estate_Sales_Date_Recorded.csv")
    staging_path_list_year = str(BASE_DIR / "data/staging/Real_Estate_Sales_List_Year.csv")
    staging_path_property_type = str(BASE_DIR / "data/staging/Real_Estate_Sales_Property_Type.csv")

    geo_coordinates_lookup_path = str(BASE_DIR / "data/raw/statewide-addresses-state.geojson")
    metrics_file_path = str(BASE_DIR / "data/output/model_evaluation_metrics.csv")

    #Remove unneeded rows
    staging_path = remove_empty_entries(main_path, staging_path, column_name="Date Recorded")


    #Split the csv for staging
    split_result1 = split_columns.override(task_id="split_coordinates")(staging_path, staging_path_coordinates, staging_path, ["Location", "Address", "Town"])
    staging_path_coordinates = split_result1["selected"]
    staging_path = split_result1["remaining"]

    split_result2 = split_columns.override(task_id="split_date")(staging_path, staging_path_date_recorded, staging_path,
                                                             ["Date Recorded"])
    staging_path_date_recorded = split_result2["selected"]
    staging_path = split_result2["remaining"]

    split_result3 = split_columns.override(task_id="split_list_year")(staging_path, staging_path_list_year, staging_path,
                                                         ["List Year"])
    staging_path_list_year = split_result3["selected"]
    staging_path = split_result3["remaining"]

    split_result4 = split_columns.override(task_id="split_property")(staging_path, staging_path_property_type, staging_path,
                                                             ["Property Type", "Residential Type"])
    staging_path_property_type = split_result4["selected"]
    staging_path = split_result4["remaining"]

    # Fix Coordinates
    staging_path_coordinates = fill_missing_location(staging_path_coordinates, staging_path_coordinates)
    staging_path_coordinates = extract_coordinates(staging_path_coordinates, staging_path_coordinates)
    staging_path_coordinates = remove_columns(staging_path_coordinates, staging_path_coordinates, "Location")
    staging_path_coordinates = fill_coordinates_from_geojson(staging_path_coordinates, geojson_path=str(geo_coordinates_lookup_path), output_path=staging_path_coordinates)

    # Date Recorded branch
    staging_path_date_recorded = correct_wrong_years(staging_path_date_recorded, staging_path_date_recorded, date_column_name="Date Recorded")
    staging_path_date_recorded = fix_date(staging_path_date_recorded, staging_path_date_recorded, date_column_name="Date Recorded")

    # List Year branch
    staging_path_list_year = year_to_jan_first(staging_path_list_year, staging_path_list_year, "List Year")
    staging_path_list_year = rename_column(staging_path_list_year, staging_path_list_year, "List Year", "List Date")

    #Fix Property Type and Residential Type
    staging_path_property_type = update_property_type(staging_path_property_type, staging_path_property_type, "Property Type", "Residential Type")
    staging_path_property_type = remove_columns.override(task_id="remove_residential_type")(staging_path_property_type, staging_path_property_type, "Residential Type")

    #remove unneeded columns
    staging_path = remove_columns.override(task_id="remove_unneeded_columns")(staging_path, staging_path, ["Non Use Code", "Assessor Remarks", "OPM remarks"])

    #merge all files together in the clean output file
    output_path = merge_files([staging_path, staging_path_property_type, staging_path_list_year, staging_path_date_recorded, staging_path_coordinates],
               output_path)


    metrics_csv_path, cleaned_csv_path = generate_ml_map_layer_with_metrics(
        cleaned_csv_path=output_path,
        geojson_output_path=geojson_layer_path,
        metrics_csv_path=metrics_file_path
    )



clean_real_estate_pipeline()