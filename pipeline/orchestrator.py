from __future__ import annotations

import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
from prefect import flow, get_run_logger
from prefect.context import get_run_context
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import ETLConfig
from models import PipelineRun, init_db
from pipeline.extractor import extract_task
from pipeline.transformer import transform_task, TransformFn
from pipeline.loader import load_task
from alerts.alerter import Alerter
from quarantine.store import QuarantineStore
from agents.autonomous_loop import AutonomousHealingLoop
from observability.event_bus import EventBus, ObservabilityEvent
from observability.traces import new_span_id, new_trace_id

logger = logging.getLogger(__name__)


@flow(
    name="self-healing-etl",
    description="ETL pipeline with schema drift detection, auto-healing, and quarantine",
    retries=0,          # flow-level retries disabled; task-level retries handle transients
    log_prints=True,
)
def etl_flow(
    source_name: str,
    source_type: str,                          # csv | jsonl | dataframe
    destination_type: str,                     # csv | jsonl | db | memory
    config: ETLConfig | None = None,
    source_path: str | None = None,
    source_df: pd.DataFrame | None = None,
    destination_path: str | None = None,
    destination_table: str | None = None,
    custom_transform: TransformFn | None = None,
) -> dict:
    """
    Top-level ETL flow.

    Returns a run summary dict with counts and status.
    """
    cfg = config or ETLConfig()
    run_id = _run_id()
    trace_id = new_trace_id()
    plog = get_run_logger()
    plog.info("Starting ETL run %s for source '%s'", run_id, source_name)

    # Track run in the quarantine DB (reuse same SQLite for simplicity)
    run_engine = init_db(cfg.quarantine.db_url)
    alerter = Alerter(
        slack_webhook_url=cfg.alerts.slack_webhook_url,
        min_severity=cfg.alerts.min_severity,
    )
    event_bus = EventBus(cfg.quarantine.db_url)
    event_bus.emit(
        run_id=run_id,
        pipeline_name=cfg.pipeline_name,
        event_type="PipelineStarted",
        severity="INFO",
        component="orchestrator",
        message=f"Pipeline started for source {source_name}",
        trace_id=trace_id,
        span_id=new_span_id("pipeline"),
        metadata={"source_type": source_type, "destination_type": destination_type},
    )

    pipeline_run = PipelineRun(
        run_id=run_id,
        pipeline_name=cfg.pipeline_name,
        source_name=source_name,
        status="RUNNING",
    )
    with Session(run_engine) as session:
        session.add(pipeline_run)
        session.commit()

    summary = {
        "run_id": run_id,
        "source": source_name,
        "status": "UNKNOWN",
        "rows_extracted": 0,
        "rows_loaded": 0,
        "rows_quarantined": 0,
        "schema_evolved": False,
        "drift_detected": False,
        "trace_id": trace_id,
    }

    try:
        # ── Extract ──────────────────────────────────────────────────
        batches = extract_task(
            source_type=source_type,
            source_path=source_path,
            source_df=source_df,
            batch_size=cfg.batch_size,
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_db_url=cfg.quarantine.db_url,
            trace_id=trace_id,
        )
        summary["rows_extracted"] = sum(len(b) for b in batches)

        # ── Transform + Heal ─────────────────────────────────────────
        t_result = transform_task(
            batches=batches,
            source_name=source_name,
            run_id=run_id,
            config=cfg,
            custom_transform=custom_transform,
            trace_id=trace_id,
        )
        summary["rows_quarantined"] = t_result.rows_quarantined
        summary["drift_detected"] = any(r.has_drift for r in t_result.drift_reports)
        summary["schema_evolved"] = t_result.schema_evolved

        # ── Load ─────────────────────────────────────────────────────
        dest_engine = None
        if destination_type == "db" and destination_path:
            dest_engine = create_engine(destination_path)

        rows_loaded = load_task(
            batches=t_result.clean_batches,
            destination_type=destination_type,
            destination_path=destination_path,
            db_engine=dest_engine,
            table_name=destination_table,
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_db_url=cfg.quarantine.db_url,
            trace_id=trace_id,
        )
        summary["rows_loaded"] = rows_loaded
        summary["status"] = "SUCCESS"
        _maybe_autonomous_recovery(
            cfg=cfg,
            event_bus=event_bus,
            run_id=run_id,
            trace_id=trace_id,
            source_name=source_name,
            summary=summary,
            context={
                "summary": dict(summary),
                "batch_size": cfg.batch_size,
                "source_type": source_type,
                "destination_type": destination_type,
                "destination_path": destination_path,
                "destination_table": destination_table,
            },
        )
        event_bus.emit(
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_type="PipelineCompleted",
            severity="INFO",
            component="orchestrator",
            message="Pipeline completed",
            trace_id=trace_id,
            span_id=new_span_id("pipeline-completed"),
            metadata=dict(summary),
        )

        plog.info(
            "ETL run %s complete: extracted=%d loaded=%d quarantined=%d drift=%s evolved=%s",
            run_id,
            summary["rows_extracted"],
            summary["rows_loaded"],
            summary["rows_quarantined"],
            summary["drift_detected"],
            summary["schema_evolved"],
        )

    except Exception as exc:
        summary["status"] = "FAILED"
        summary["error"] = str(exc)
        plog.error("ETL run %s FAILED: %s", run_id, exc)
        failure_event = event_bus.emit(
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_type="PipelineFailed",
            severity="ERROR",
            component="orchestrator",
            message=str(exc),
            trace_id=trace_id,
            span_id=new_span_id("pipeline-failed"),
            metadata={
                "source_type": source_type,
                "destination_type": destination_type,
                "source_path": source_path,
                "destination_path": destination_path,
                "error": type(exc).__name__,
            },
        )
        alerter.pipeline_failure_alert(
            pipeline_name=cfg.pipeline_name,
            source_name=source_name,
            run_id=run_id,
            error_message=str(exc),
            root_cause_hints=[
                "Check Prefect task logs for the full traceback.",
                "Verify source file/connection is accessible.",
                f"Source: {source_type}  Destination: {destination_type}",
            ],
        )
        if cfg.autonomous.enable_autonomous_healing:
            loop = AutonomousHealingLoop(cfg.quarantine.db_url, cfg.autonomous)
            loop_result = loop.handle_failure(
                failure_event,
                context={
                    "summary": dict(summary),
                    "batch_size": cfg.batch_size,
                    "source_type": source_type,
                    "destination_type": destination_type,
                    "source_path": source_path,
                    "destination_path": destination_path,
                    "destination_table": destination_table,
                    "destination_reachable": False,
                },
            )
            summary["autonomous_healing"] = loop_result.as_dict()
            if loop_result.escalated:
                summary["status"] = "ESCALATED"
            return summary
        raise

    finally:
        # Persist final run state
        with Session(run_engine) as session:
            from sqlalchemy import select, update
            session.execute(
                update(PipelineRun)
                .where(PipelineRun.run_id == run_id)
                .values(
                    status=summary["status"],
                    rows_extracted=summary["rows_extracted"],
                    rows_loaded=summary.get("rows_loaded", 0),
                    rows_quarantined=summary["rows_quarantined"],
                    drift_detected=summary["drift_detected"],
                    finished_at=datetime.now(timezone.utc),
                    error_message=summary.get("error"),
                )
            )
            session.commit()

    return summary


def _maybe_autonomous_recovery(
    *,
    cfg: ETLConfig,
    event_bus: EventBus,
    run_id: str,
    trace_id: str,
    source_name: str,
    summary: dict,
    context: dict,
) -> None:
    if not cfg.autonomous.enable_autonomous_healing:
        return
    if not summary.get("drift_detected") and not summary.get("rows_quarantined"):
        return

    event_type = "QuarantineTriggered" if summary.get("rows_quarantined") else "SchemaDriftDetected"
    severity = "ERROR" if summary.get("rows_quarantined") else "WARNING"
    event = event_bus.emit(
        run_id=run_id,
        pipeline_name=cfg.pipeline_name,
        event_type=event_type,
        severity=severity,
        component="orchestrator",
        message=(
            f"Autonomous review triggered for source {source_name}: "
            f"drift={summary.get('drift_detected')} quarantined={summary.get('rows_quarantined')}"
        ),
        trace_id=trace_id,
        span_id=new_span_id("autonomous-trigger"),
        metadata=dict(summary),
    )
    loop = AutonomousHealingLoop(cfg.quarantine.db_url, cfg.autonomous)
    result = loop.handle_failure(event, context=context)
    summary["autonomous_healing"] = result.as_dict()


def _run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"run-{ts}-{uuid.uuid4().hex[:6]}"
