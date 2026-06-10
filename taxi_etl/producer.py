from __future__ import annotations

import csv
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

from taxi_etl.config import TaxiDriftConfig, TaxiRuntimeConfig, TaxiSyntheticConfig


def run_producer(
    incoming_dir: str | Path,
    source_csv: str | Path | None = None,
    records: int = 30,
    interval_seconds: float = 1.0,
    batch_size: int = 1,
    inject_drift: bool = True,
    runtime: TaxiRuntimeConfig | None = None,
    random_failure_rate: float = 0.0,
    random_failure_seed: int = 2026,
) -> int:
    """Write taxi trip micro-batches to the incoming folder."""
    incoming = Path(incoming_dir)
    incoming.mkdir(parents=True, exist_ok=True)

    runtime = runtime or TaxiRuntimeConfig()
    drift = runtime.drift if inject_drift else TaxiDriftConfig(enabled=False)
    failure_rng = random.Random(random_failure_seed)
    stream = iter_source_records(source_csv, records, runtime=runtime, drift=drift)
    written = 0
    batch: list[dict] = []
    for record in stream:
        if random_failure_rate > 0 and failure_rng.random() < random_failure_rate:
            record = _apply_random_failure(record, failure_rng)
        batch.append(record)
        if len(batch) >= batch_size:
            _write_jsonl(incoming / f"taxi_trip_{written + 1:06d}.jsonl", batch)
            written += len(batch)
            batch = []
            if interval_seconds > 0:
                time.sleep(interval_seconds)

    if batch:
        _write_jsonl(incoming / f"taxi_trip_{written + 1:06d}.jsonl", batch)
        written += len(batch)

    return written


def iter_source_records(
    source_csv: str | Path | None,
    limit: int,
    runtime: TaxiRuntimeConfig | None = None,
    drift: TaxiDriftConfig | None = None,
) -> Iterable[dict]:
    runtime = runtime or TaxiRuntimeConfig()
    drift = drift or runtime.drift
    if source_csv:
        yield from _iter_csv_records(Path(source_csv), limit, runtime, drift)
        return

    yield from _synthetic_records(limit, runtime.synthetic, drift)


def _iter_csv_records(
    path: Path,
    limit: int,
    runtime: TaxiRuntimeConfig,
    drift: TaxiDriftConfig,
) -> Iterable[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader, start=1):
            if idx > limit:
                break
            record = _normalize_source_record(row, idx, runtime)
            if _should_apply_drift(idx, limit, drift):
                record = _apply_drift(record, drift)
            yield record


def _synthetic_records(
    limit: int,
    synthetic: TaxiSyntheticConfig,
    drift: TaxiDriftConfig,
) -> Iterable[dict]:
    rng = random.Random(synthetic.seed)
    for idx in range(1, limit + 1):
        pickup = synthetic.start_time + timedelta(seconds=idx)
        duration = rng.randint(synthetic.min_duration_minutes, synthetic.max_duration_minutes)
        distance = round(rng.uniform(synthetic.min_distance, synthetic.max_distance), 2)
        fare = round(
            synthetic.base_fare
            + distance * rng.uniform(synthetic.min_fare_per_mile, synthetic.max_fare_per_mile),
            2,
        )
        tip = round(fare * rng.choice(synthetic.tip_rates), 2)
        record = {
            "trip_id": idx,
            "VendorID": rng.choice(synthetic.vendor_ids),
            "pickup_datetime": pickup.isoformat(sep=" "),
            "dropoff_datetime": (pickup + timedelta(minutes=duration)).isoformat(sep=" "),
            "passenger_count": rng.randint(1, 4),
            "trip_distance": distance,
            "fare_amount": fare,
            "tip_amount": tip,
            "payment_type": rng.choice(synthetic.payment_types),
        }
        if _should_apply_drift(idx, limit, drift):
            record = _apply_drift(record, drift)
        yield record


def _normalize_source_record(
    row: dict,
    fallback_trip_id: int,
    runtime: TaxiRuntimeConfig,
) -> dict:
    aliases = runtime.schema.column_aliases
    normalized = {target: row[source] for source, target in aliases.items() if source in row}
    return {
        "trip_id": row.get("trip_id") or fallback_trip_id,
        "VendorID": row.get("VendorID") or row.get("vendor_id") or normalized.get("vendor_id"),
        "pickup_datetime": normalized.get("pickup_datetime") or row.get("pickup_datetime"),
        "dropoff_datetime": normalized.get("dropoff_datetime") or row.get("dropoff_datetime"),
        "passenger_count": row.get("passenger_count"),
        "trip_distance": row.get("trip_distance"),
        "fare_amount": row.get("fare_amount"),
        "tip_amount": row.get("tip_amount"),
        "payment_type": row.get("payment_type"),
    }


def _should_apply_drift(record_index: int, total_records: int, drift: TaxiDriftConfig) -> bool:
    return drift.enabled and record_index > drift.starts_after(total_records)


def _apply_drift(record: dict, drift: TaxiDriftConfig) -> dict:
    drifted = dict(record)
    for column in drift.currency_columns:
        if column in drifted and drifted[column] is not None:
            drifted[column] = f"${float(drifted[column]):.2f}"
    for mutation in drift.added_columns:
        drifted[mutation.column] = mutation.value
    return drifted


def _apply_random_failure(record: dict, rng: random.Random) -> dict:
    failed = dict(record)
    failure = rng.choice(
        [
            "schema_drift",
            "type_drift",
            "malformed_record",
            "quality_failure",
            "resource_pressure_marker",
        ]
    )
    if failure == "schema_drift":
        failed["driver_rating"] = "new-field"
    elif failure == "type_drift":
        failed["trip_distance"] = "not-a-distance"
    elif failure == "malformed_record":
        failed["pickup_datetime"] = "malformed-date"
    elif failure == "quality_failure":
        failed["fare_amount"] = -1
    elif failure == "resource_pressure_marker":
        failed["payload_blob"] = "x" * 5000
    failed["_injected_failure"] = failure
    return failed


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, default=str) + "\n")


def preview_records(records: int = 5) -> pd.DataFrame:
    runtime = TaxiRuntimeConfig()
    drift = TaxiDriftConfig(enabled=False)
    return pd.DataFrame(list(_synthetic_records(records, runtime.synthetic, drift)))
