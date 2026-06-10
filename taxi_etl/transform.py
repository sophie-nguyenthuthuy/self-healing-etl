from __future__ import annotations

import pandas as pd

from taxi_etl.config import TaxiRuntimeConfig


def transform_taxi_trips(
    df: pd.DataFrame,
    runtime: TaxiRuntimeConfig | None = None,
) -> pd.DataFrame:
    """Clean, validate, and enrich taxi trip records."""
    if df.empty:
        return df

    runtime = runtime or TaxiRuntimeConfig()
    schema = runtime.schema
    clean = df.copy()
    clean = clean.rename(columns=schema.column_aliases)

    for column in schema.canonical_columns:
        if column not in clean.columns:
            clean[column] = pd.NA

    clean = clean.drop_duplicates(subset=["trip_id"], keep="last")
    clean["pickup_datetime"] = pd.to_datetime(clean["pickup_datetime"], errors="coerce")
    clean["dropoff_datetime"] = pd.to_datetime(clean["dropoff_datetime"], errors="coerce")

    for column in schema.numeric_columns:
        clean[column] = (
            clean[column]
            .astype("string")
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
        )
        clean[column] = pd.to_numeric(clean[column], errors="coerce")

    for column, default in schema.nullable_defaults.items():
        clean[column] = clean[column].fillna(default)
    clean["payment_type"] = clean["payment_type"].astype("string")

    valid = (
        clean["pickup_datetime"].notna()
        & clean["dropoff_datetime"].notna()
        & (clean["pickup_datetime"] < clean["dropoff_datetime"])
    )
    for column in schema.required_positive_columns:
        valid &= clean[column] > 0
    clean = clean.loc[valid].copy()

    clean["trip_duration"] = (
        clean["dropoff_datetime"] - clean["pickup_datetime"]
    ).dt.total_seconds() / 60
    clean["revenue"] = clean["fare_amount"] + clean["tip_amount"]
    clean["pickup_hour"] = clean["pickup_datetime"].dt.hour
    clean["pickup_date"] = clean["pickup_datetime"].dt.date.astype("string")

    ordered = list(schema.canonical_columns) + [
        "trip_duration",
        "revenue",
        "pickup_hour",
        "pickup_date",
    ]
    extras = [column for column in clean.columns if column not in ordered]
    return clean[ordered + extras]
