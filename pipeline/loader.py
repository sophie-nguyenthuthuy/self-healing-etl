from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE
from sqlalchemy import inspect, text

from observability.event_bus import EventBus
from observability.traces import new_span_id

logger = logging.getLogger(__name__)


@task(name="load", retries=2, retry_delay_seconds=15, log_prints=True, cache_policy=NO_CACHE)
def load_task(
    batches: list[pd.DataFrame],
    destination_type: str,
    destination_path: str | None = None,
    db_engine=None,
    table_name: str | None = None,
    if_exists: str = "append",
    run_id: str | None = None,
    pipeline_name: str | None = None,
    event_db_url: str | None = None,
    trace_id: str | None = None,
) -> int:
    """
    Write clean batches to the configured destination.

    Supported destinations:
      csv    — append rows to a CSV file
      jsonl  — append rows to a JSONL file
      db     — write to a SQLAlchemy-compatible database table
      memory — no-op, return count (useful in tests)
    """
    bus = EventBus(event_db_url) if event_db_url else None
    if bus and run_id and pipeline_name and trace_id:
        bus.emit(
            run_id=run_id,
            pipeline_name=pipeline_name,
            event_type="LoadStarted",
            severity="INFO",
            component="loader",
            message=f"Loading to {destination_type}",
            trace_id=trace_id,
            span_id=new_span_id("load"),
            metadata={"destination_type": destination_type, "destination_path": destination_path, "table_name": table_name},
        )

    total_loaded = 0

    if not batches:
        logger.warning("No batches to load")
        return 0

    try:
        if destination_type == "csv":
            total_loaded = _load_csv(batches, destination_path)
        elif destination_type == "jsonl":
            total_loaded = _load_jsonl(batches, destination_path)
        elif destination_type == "db":
            total_loaded = _load_db(batches, db_engine, table_name, if_exists)
        elif destination_type == "memory":
            total_loaded = sum(len(b) for b in batches)
            logger.info("Memory sink: %d rows (no-op)", total_loaded)
        else:
            raise ValueError(f"Unknown destination_type: {destination_type!r}")
    except Exception as exc:
        if bus and run_id and pipeline_name and trace_id:
            bus.emit(
                run_id=run_id,
                pipeline_name=pipeline_name,
                event_type="LoadFailed",
                severity="ERROR",
                component="loader",
                message=str(exc),
                trace_id=trace_id,
                span_id=new_span_id("load-failed"),
                metadata={"destination_type": destination_type, "destination_path": destination_path, "error": type(exc).__name__},
            )
        raise

    logger.info("Loaded %d rows to '%s'", total_loaded, destination_type)
    if bus and run_id and pipeline_name and trace_id:
        bus.emit(
            run_id=run_id,
            pipeline_name=pipeline_name,
            event_type="LoadCompleted",
            severity="INFO",
            component="loader",
            message=f"Loaded {total_loaded} rows",
            trace_id=trace_id,
            span_id=new_span_id("load-completed"),
            metadata={"rows_loaded": total_loaded, "destination_type": destination_type},
        )
    return total_loaded


def _load_csv(batches: list[pd.DataFrame], path: str) -> int:
    dest = Path(path)
    write_header = not dest.exists()
    total = 0
    with open(dest, "a", newline="") as fh:
        for batch in batches:
            batch.to_csv(fh, header=write_header, index=False)
            write_header = False
            total += len(batch)
    return total


def _load_jsonl(batches: list[pd.DataFrame], path: str) -> int:
    dest = Path(path)
    total = 0
    with open(dest, "a") as fh:
        for batch in batches:
            for record in batch.to_dict(orient="records"):
                fh.write(json.dumps(record, default=str) + "\n")
            total += len(batch)
    return total


def _load_db(
    batches: list[pd.DataFrame],
    engine,
    table_name: str,
    if_exists: str,
) -> int:
    total = 0
    first = True
    with engine.begin() as conn:
        for batch in batches:
            mode = if_exists if first else "append"
            if mode == "append":
                _ensure_table_columns(conn, table_name, batch)
            batch.to_sql(table_name, conn, if_exists=mode, index=False)
            first = False
            total += len(batch)
    return total


def _ensure_table_columns(conn, table_name: str, batch: pd.DataFrame) -> None:
    inspector = inspect(conn)
    if not inspector.has_table(table_name):
        return

    existing = {column["name"] for column in inspector.get_columns(table_name)}
    missing = [column for column in batch.columns if column not in existing]
    preparer = conn.dialect.identifier_preparer
    quoted_table = preparer.quote(table_name)
    for column in missing:
        conn.execute(text(f"ALTER TABLE {quoted_table} ADD COLUMN {preparer.quote(column)} TEXT"))
