from __future__ import annotations

import logging
import shutil
import time
import uuid
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from agents.autonomous_loop import AutonomousHealingLoop
from observability.event_bus import EventBus
from observability.traces import new_span_id, new_trace_id
from pipeline.orchestrator import etl_flow
from quarantine.store import QuarantineStore
from taxi_etl.config import TaxiDriftConfig, TaxiETLPaths, TaxiRuntimeConfig, taxi_etl_config
from taxi_etl.producer import run_producer
from taxi_etl.transform import transform_taxi_trips
from taxi_etl.warehouse import ensure_parent_dir, load_raw_trips, read_live_metrics, refresh_analytics

logger = logging.getLogger(__name__)
console = Console()


def run_taxi_demo(
    records: int = 30,
    batch_size: int = 5,
    source_csv: str | None = None,
    warehouse_url: str | None = None,
    taxi_root: str | Path | None = None,
    inject_drift: bool = True,
    drift_start_after: int | None = None,
    autonomous_mode: bool = False,
    require_human_approval: bool = False,
    random_failures: bool = False,
) -> dict:
    paths = TaxiETLPaths(root=Path(taxi_root)) if taxi_root else TaxiETLPaths()
    runtime = _runtime_config(inject_drift=inject_drift, drift_start_after=drift_start_after)
    paths.ensure()
    warehouse = warehouse_url or paths.warehouse_url
    ensure_parent_dir(warehouse)

    produced = run_producer(
        incoming_dir=paths.incoming_dir,
        source_csv=source_csv,
        records=records,
        interval_seconds=0,
        batch_size=batch_size,
        inject_drift=inject_drift,
        runtime=runtime,
        random_failure_rate=0.15 if (random_failures or autonomous_mode) else 0.0,
    )
    result = run_realtime_etl(
        paths=paths,
        warehouse_url=warehouse,
        runtime=runtime,
        max_files=None,
        poll_seconds=0,
        stop_when_idle=True,
        autonomous_mode=autonomous_mode,
        require_human_approval=require_human_approval,
    )
    result["records_produced"] = produced
    _print_taxi_summary(result, warehouse, paths, runtime)
    return result


def run_realtime_etl(
    paths: TaxiETLPaths | None = None,
    warehouse_url: str | None = None,
    taxi_root: str | Path | None = None,
    runtime: TaxiRuntimeConfig | None = None,
    max_files: int | None = None,
    poll_seconds: float = 1.0,
    stop_when_idle: bool = False,
    autonomous_mode: bool = False,
    require_human_approval: bool = False,
) -> dict:
    paths = paths or (TaxiETLPaths(root=Path(taxi_root)) if taxi_root else TaxiETLPaths())
    runtime = runtime or TaxiRuntimeConfig()
    paths.ensure()
    warehouse = warehouse_url or paths.warehouse_url
    ensure_parent_dir(warehouse)
    cfg = taxi_etl_config(
        paths,
        runtime,
        autonomous_mode=autonomous_mode,
        require_human_approval=require_human_approval,
    )

    processed_files = 0
    loaded_rows = 0
    quarantined_rows = 0
    last_summary: dict | None = None

    while True:
        files = sorted(paths.incoming_dir.glob("*.jsonl")) + sorted(paths.incoming_dir.glob("*.csv"))
        if not files:
            if stop_when_idle:
                break
            time.sleep(poll_seconds)
            continue

        for file_path in files:
            try:
                summary = _process_file(file_path, paths, warehouse, cfg, runtime)
                processed_files += 1
                loaded_rows += summary.get("rows_loaded", 0)
                quarantined_rows += summary.get("rows_quarantined", 0)
                last_summary = summary
            except Exception:
                logger.exception("Failed to process incoming taxi file %s", file_path)
                if autonomous_mode:
                    trace_id = new_trace_id()
                    run_id = f"taxi-watch-{uuid.uuid4().hex[:8]}"
                    event = EventBus(cfg.quarantine.db_url).emit(
                        run_id=run_id,
                        pipeline_name=cfg.pipeline_name,
                        event_type="LoadFailed",
                        severity="ERROR",
                        component="taxi_watcher",
                        message=f"Failed to process incoming taxi file {file_path}",
                        trace_id=trace_id,
                        span_id=new_span_id("taxi-watcher"),
                        metadata={"file_path": str(file_path), "warehouse_url": warehouse},
                    )
                    AutonomousHealingLoop(cfg.quarantine.db_url, cfg.autonomous).handle_failure(
                        event,
                        context={
                            "summary": {"rows_loaded": 0, "rows_quarantined": 0},
                            "destination_reachable": False,
                            "source_path": str(file_path),
                            "destination_path": warehouse,
                        },
                    )
                _move_file(file_path, paths.failed_dir / file_path.name)
            if max_files is not None and processed_files >= max_files:
                return {
                    "files_processed": processed_files,
                    "rows_loaded": loaded_rows,
                    "rows_quarantined": quarantined_rows,
                    "last_run": last_summary,
                }

    return {
        "files_processed": processed_files,
        "rows_loaded": loaded_rows,
        "rows_quarantined": quarantined_rows,
        "last_run": last_summary,
    }


def _process_file(
    file_path: Path,
    paths: TaxiETLPaths,
    warehouse_url: str,
    cfg,
    runtime: TaxiRuntimeConfig,
) -> dict:
    raw = _read_incoming_file(file_path)
    tables = runtime.schema.table_names
    load_raw_trips(raw, warehouse_url, tables)
    summary = etl_flow(
        source_name=runtime.schema.source_name,
        source_type="dataframe",
        destination_type="db",
        destination_path=warehouse_url,
        destination_table=tables.fact,
        config=cfg,
        source_df=raw,
        custom_transform=transform_taxi_trips,
    )
    refresh_analytics(warehouse_url, runtime)
    _move_file(file_path, paths.processed_dir / file_path.name)
    return summary


def _read_incoming_file(file_path: Path) -> pd.DataFrame:
    if file_path.suffix == ".jsonl":
        return pd.read_json(file_path, lines=True)
    if file_path.suffix == ".csv":
        return pd.read_csv(file_path)
    raise ValueError(f"Unsupported incoming taxi file: {file_path}")


def _print_taxi_summary(
    result: dict,
    warehouse_url: str,
    paths: TaxiETLPaths,
    runtime: TaxiRuntimeConfig,
) -> None:
    metrics = read_live_metrics(warehouse_url, runtime)
    stats = QuarantineStore(paths.quarantine_url).stats(
        pipeline_name=runtime.schema.pipeline_name
    )

    table = Table(title="Real-Time Taxi ETL Summary", show_header=True, header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Records produced", str(result.get("records_produced", 0)))
    table.add_row("Files processed", str(result["files_processed"]))
    table.add_row("Rows loaded", str(result["rows_loaded"]))
    table.add_row("Rows quarantined", str(result["rows_quarantined"]))
    table.add_row("Total trips", str(metrics["total_trips"]))
    table.add_row("Total revenue", f"${metrics['total_revenue']:.2f}")
    table.add_row("Average fare", f"${metrics['average_fare']:.2f}")
    table.add_row("Average distance", f"{metrics['average_trip_distance']:.2f}")
    table.add_row("MTTD", _format_seconds(stats["mttd_seconds"]))
    table.add_row("MTTR", _format_seconds(stats["mttr_seconds"]))
    console.print(table)


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.3f}s"


def _runtime_config(inject_drift: bool, drift_start_after: int | None) -> TaxiRuntimeConfig:
    base = TaxiRuntimeConfig()
    drift = TaxiDriftConfig(
        enabled=inject_drift,
        start_after_record=drift_start_after,
        start_fraction=base.drift.start_fraction,
        currency_columns=base.drift.currency_columns,
        added_columns=base.drift.added_columns,
    )
    return TaxiRuntimeConfig(schema=base.schema, synthetic=base.synthetic, drift=drift)


def _move_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(source), destination)
    except PermissionError:
        shutil.copy2(source, destination)
        try:
            source.unlink()
        except PermissionError:
            logger.warning("Copied %s to %s but could not delete original due to permissions", source, destination)
