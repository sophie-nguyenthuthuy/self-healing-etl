from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE

from observability.event_bus import EventBus
from observability.traces import new_span_id

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Source protocol — any callable that returns a DataFrame iterable
# ------------------------------------------------------------------

def csv_source(file_path: str | Path, batch_size: int = 1_000) -> Iterator[pd.DataFrame]:
    """Yield DataFrames from a CSV file in configurable batches."""
    path = Path(file_path)
    logger.info("Extracting from CSV: %s", path)
    for chunk in pd.read_csv(path, chunksize=batch_size, dtype_backend="numpy_nullable"):
        yield chunk


def jsonl_source(file_path: str | Path, batch_size: int = 1_000) -> Iterator[pd.DataFrame]:
    """Yield DataFrames from a newline-delimited JSON file."""
    path = Path(file_path)
    logger.info("Extracting from JSONL: %s", path)
    buffer: list[dict[str, Any]] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            buffer.append(json.loads(line))
            if len(buffer) >= batch_size:
                yield pd.DataFrame(buffer)
                buffer = []
    if buffer:
        yield pd.DataFrame(buffer)


def dataframe_source(df: pd.DataFrame, batch_size: int = 1_000) -> Iterator[pd.DataFrame]:
    """Yield batches from an in-memory DataFrame (useful for testing)."""
    for start in range(0, len(df), batch_size):
        yield df.iloc[start : start + batch_size].copy()


# ------------------------------------------------------------------
# Prefect task wrapper
# ------------------------------------------------------------------

@task(name="extract", retries=2, retry_delay_seconds=10, log_prints=True, cache_policy=NO_CACHE)
def extract_task(
    source_type: str,
    source_path: str | None,
    source_df: pd.DataFrame | None,
    batch_size: int,
    run_id: str | None = None,
    pipeline_name: str | None = None,
    event_db_url: str | None = None,
    trace_id: str | None = None,
) -> list[pd.DataFrame]:
    """
    Load all batches from the configured source into memory.
    For large datasets, prefer streaming directly to the transform step,
    but Prefect task results must be serializable, so we collect here.
    """
    bus = _bus(event_db_url)
    if bus and run_id and pipeline_name and trace_id:
        bus.emit(
            run_id=run_id,
            pipeline_name=pipeline_name,
            event_type="BatchReceived",
            severity="INFO",
            component="extractor",
            message=f"Starting extraction from {source_type}",
            trace_id=trace_id,
            span_id=new_span_id("extract"),
            metadata={"source_type": source_type, "source_path": source_path, "batch_size": batch_size},
        )

    batches: list[pd.DataFrame] = []

    try:
        if source_type == "csv":
            for batch in csv_source(source_path, batch_size):
                batches.append(batch)
        elif source_type == "jsonl":
            for batch in jsonl_source(source_path, batch_size):
                batches.append(batch)
        elif source_type == "dataframe":
            for batch in dataframe_source(source_df, batch_size):
                batches.append(batch)
        else:
            raise ValueError(f"Unknown source_type: {source_type!r}")
    except Exception as exc:
        if bus and run_id and pipeline_name and trace_id:
            bus.emit(
                run_id=run_id,
                pipeline_name=pipeline_name,
                event_type="ExtractFailed",
                severity="ERROR",
                component="extractor",
                message=str(exc),
                trace_id=trace_id,
                span_id=new_span_id("extract-failed"),
                metadata={"source_type": source_type, "source_path": source_path, "error": type(exc).__name__},
            )
        raise

    total = sum(len(b) for b in batches)
    logger.info("Extracted %d rows in %d batch(es) from '%s'", total, len(batches), source_type)
    if bus and run_id and pipeline_name and trace_id:
        bus.emit(
            run_id=run_id,
            pipeline_name=pipeline_name,
            event_type="ExtractCompleted",
            severity="INFO",
            component="extractor",
            message=f"Extracted {total} rows in {len(batches)} batch(es)",
            trace_id=trace_id,
            span_id=new_span_id("extract-completed"),
            metadata={"rows_extracted": total, "batches": len(batches)},
        )
    return batches


def _bus(db_url: str | None) -> EventBus | None:
    return EventBus(db_url) if db_url else None
