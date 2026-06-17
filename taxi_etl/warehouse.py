from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from taxi_etl.config import TaxiRuntimeConfig, TaxiWarehouseTables


def load_raw_trips(
    df: pd.DataFrame,
    warehouse_url: str,
    tables: TaxiWarehouseTables | None = None,
) -> int:
    if df.empty:
        return 0
    tables = tables or TaxiWarehouseTables()
    engine = create_engine(warehouse_url)
    with engine.begin() as conn:
        _ensure_table_columns(conn, tables.raw, df)
        df.to_sql(tables.raw, conn, if_exists="append", index=False)
    return len(df)


def refresh_analytics(
    warehouse_url: str,
    runtime: TaxiRuntimeConfig | None = None,
) -> dict[str, int]:
    runtime = runtime or TaxiRuntimeConfig()
    tables = runtime.schema.table_names
    engine = create_engine(warehouse_url)
    with engine.begin() as conn:
        if not _table_exists(conn, tables.fact):
            return {"fact_rows": 0, "summary_rows": 0}

        fact = pd.read_sql_table(tables.fact, conn)
        if fact.empty:
            return {"fact_rows": 0, "summary_rows": 0}

        fact["pickup_datetime"] = pd.to_datetime(fact["pickup_datetime"], errors="coerce")
        fact["pickup_date"] = fact["pickup_datetime"].dt.date.astype("string")
        fact["pickup_hour"] = pd.to_numeric(fact["pickup_hour"], errors="coerce").fillna(0).astype(int)

        _replace_table(conn, tables.dim_date, _date_dimension(fact))
        _replace_table(conn, tables.dim_payment, _payment_dimension(fact))

        summary = (
            fact.groupby(["pickup_date", "pickup_hour", "payment_type"], dropna=False)
            .agg(
                total_trips=("trip_id", "count"),
                total_revenue=("revenue", "sum"),
                average_fare=("fare_amount", "mean"),
                average_trip_distance=("trip_distance", "mean"),
                average_trip_duration=("trip_duration", "mean"),
            )
            .reset_index()
        )
        _replace_table(conn, tables.summary, summary)

    return {"fact_rows": len(fact), "summary_rows": len(summary)}


def read_live_metrics(
    warehouse_url: str,
    runtime: TaxiRuntimeConfig | None = None,
) -> dict[str, float]:
    runtime = runtime or TaxiRuntimeConfig()
    tables = runtime.schema.table_names
    engine = create_engine(warehouse_url)
    with engine.begin() as conn:
        if not _table_exists(conn, tables.fact):
            return _empty_metrics()
        fact = pd.read_sql_table(tables.fact, conn)

    if fact.empty:
        return _empty_metrics()

    return {
        "total_trips": int(len(fact)),
        "total_revenue": float(pd.to_numeric(fact["revenue"], errors="coerce").sum()),
        "average_fare": float(pd.to_numeric(fact["fare_amount"], errors="coerce").mean()),
        "average_trip_distance": float(pd.to_numeric(fact["trip_distance"], errors="coerce").mean()),
        "average_trip_duration": float(pd.to_numeric(fact["trip_duration"], errors="coerce").mean()),
    }


def read_table(warehouse_url: str, table_name: str) -> pd.DataFrame:
    engine = create_engine(warehouse_url)
    with engine.begin() as conn:
        if not _table_exists(conn, table_name):
            return pd.DataFrame()
        return pd.read_sql_table(table_name, conn)


def ensure_parent_dir(db_url: str) -> None:
    if not db_url.startswith("sqlite:///"):
        return
    db_path = Path(db_url.replace("sqlite:///", "", 1))
    if db_path.name == ":memory:":
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _date_dimension(fact: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(fact["pickup_datetime"], errors="coerce").dropna()
    dim = pd.DataFrame({"date_id": dates.dt.date.astype("string").unique()})
    dim["year"] = pd.to_datetime(dim["date_id"]).dt.year
    dim["month"] = pd.to_datetime(dim["date_id"]).dt.month
    dim["day"] = pd.to_datetime(dim["date_id"]).dt.day
    return dim


def _payment_dimension(fact: pd.DataFrame) -> pd.DataFrame:
    values = fact["payment_type"].fillna("Unknown").astype("string").drop_duplicates()
    return pd.DataFrame({"payment_type": values.sort_values().to_list()})


def _replace_table(conn, table_name: str, df: pd.DataFrame) -> None:
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def _table_exists(conn, table_name: str) -> bool:
    return inspect(conn).has_table(table_name)


def _ensure_table_columns(conn, table_name: str, df: pd.DataFrame) -> None:
    inspector = inspect(conn)
    if not inspector.has_table(table_name):
        return

    existing = {column["name"] for column in inspector.get_columns(table_name)}
    missing = [column for column in df.columns if column not in existing]
    preparer = conn.dialect.identifier_preparer
    quoted_table = preparer.quote(table_name)
    for column in missing:
        conn.execute(text(f"ALTER TABLE {quoted_table} ADD COLUMN {preparer.quote(column)} TEXT"))


def _empty_metrics() -> dict[str, float]:
    return {
        "total_trips": 0,
        "total_revenue": 0.0,
        "average_fare": 0.0,
        "average_trip_distance": 0.0,
        "average_trip_duration": 0.0,
    }
