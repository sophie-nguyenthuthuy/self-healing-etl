from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Any

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE

# project-local imports resolved via sys.path set in orchestrator
from schema.drift_detector import DriftDetector, DriftReport, schema_from_df
from schema.registry import SchemaRegistry
from quarantine.store import QuarantineStore
from healing.strategies import HealingEngine, HealingResult
from alerts.alerter import Alerter
from config import ETLConfig
from observability.event_bus import EventBus
from observability.traces import new_span_id

logger = logging.getLogger(__name__)

TransformFn = Callable[[pd.DataFrame], pd.DataFrame]


@dataclass
class TransformResult:
    clean_batches: list[pd.DataFrame] = field(default_factory=list)
    drift_reports: list[DriftReport] = field(default_factory=list)
    healing_results: list[HealingResult] = field(default_factory=list)
    rows_total: int = 0
    rows_clean: int = 0
    rows_quarantined: int = 0
    schema_evolved: bool = False


@task(name="transform", retries=1, retry_delay_seconds=5, log_prints=True, cache_policy=NO_CACHE)
def transform_task(
    batches: list[pd.DataFrame],
    source_name: str,
    run_id: str,
    config: ETLConfig,
    custom_transform: TransformFn | None = None,
    trace_id: str | None = None,
) -> TransformResult:
    registry = SchemaRegistry(config.schema_registry.db_url)
    quarantine = QuarantineStore(config.quarantine.db_url)
    detector = DriftDetector()
    healer = HealingEngine(
        enable_type_coercion=config.healing.enable_type_coercion,
        enable_column_backfill=config.healing.enable_column_backfill,
        enable_schema_evolution=config.healing.enable_schema_evolution,
        max_coercion_loss_pct=config.healing.max_coercion_loss_pct,
    )
    alerter = Alerter(
        slack_webhook_url=config.alerts.slack_webhook_url,
        min_severity=config.alerts.min_severity,
    )

    result = TransformResult()
    bus = EventBus(config.quarantine.db_url)
    trace_id = trace_id or run_id
    bus.emit(
        run_id=run_id,
        pipeline_name=config.pipeline_name,
        event_type="TransformStarted",
        severity="INFO",
        component="transformer",
        message=f"Transforming {len(batches)} batch(es)",
        trace_id=trace_id,
        span_id=new_span_id("transform"),
        metadata={"batches": len(batches), "source_name": source_name},
    )

    schema_info = registry.get_active(source_name)

    for batch_idx, batch in enumerate(batches):
        result.rows_total += len(batch)

        # --- Optional custom transform (business logic) ---
        if custom_transform is not None:
            try:
                batch = custom_transform(batch)
            except Exception as exc:
                logger.error("Custom transform failed on batch %d: %s", batch_idx, exc)
                bus.emit(
                    run_id=run_id,
                    pipeline_name=config.pipeline_name,
                    event_type="TransformFailed",
                    severity="ERROR",
                    component="transformer",
                    message=str(exc),
                    trace_id=trace_id,
                    span_id=new_span_id("transform-failed"),
                    metadata={"batch_index": batch_idx, "rows": len(batch), "error": type(exc).__name__},
                )
                _quarantine_batch(
                    quarantine, batch, config, source_name, run_id,
                    "TRANSFORM_ERROR", str(exc),
                    root_cause_hint="Custom transform function raised an exception. "
                                    "Check transform logic for data-dependent failures.",
                )
                result.rows_quarantined += len(batch)
                bus.emit(
                    run_id=run_id,
                    pipeline_name=config.pipeline_name,
                    event_type="QuarantineTriggered",
                    severity="ERROR",
                    component="quarantine",
                    message=f"Quarantined {len(batch)} transform-failed rows",
                    trace_id=trace_id,
                    span_id=new_span_id("quarantine"),
                    metadata={"rows_quarantined": len(batch), "error_type": "TRANSFORM_ERROR"},
                )
                continue

        # Ensure the baseline schema reflects the business-ready shape.
        if schema_info is None and not batch.empty:
            version = registry.register(source_name, schema_from_df(batch))
            logger.info("Auto-registered initial schema v%d for '%s'", version, source_name)
            schema_info = registry.get_active(source_name)

        # --- Drift detection ---
        if schema_info is None:
            result.clean_batches.append(batch)
            result.rows_clean += len(batch)
            continue

        exp_version, exp_schema = schema_info
        drift = detector.detect(source_name, batch, exp_version, exp_schema)
        result.drift_reports.append(drift)

        if not drift.has_drift:
            result.clean_batches.append(batch)
            result.rows_clean += len(batch)
            continue

        # --- Drift detected: log & alert ---
        drift_event_id = quarantine.log_drift_event(
            pipeline_name=config.pipeline_name,
            source_name=source_name,
            run_id=run_id,
            drift_type=",".join(drift.drift_types),
            details_json=drift.to_details_json(),
        )
        bus.emit(
            run_id=run_id,
            pipeline_name=config.pipeline_name,
            event_type="SchemaDriftDetected",
            severity="WARNING",
            component="drift_detector",
            message=drift.summary(),
            trace_id=trace_id,
            span_id=new_span_id("drift"),
            metadata={"drift_event_id": drift_event_id, "details": drift.to_details_json()},
        )

        if config.schema_registry.strict_mode:
            _quarantine_batch(
                quarantine, batch, config, source_name, run_id,
                "SCHEMA_DRIFT", drift.summary(),
                root_cause_hint="\n".join(drift.root_cause_hints()),
                schema_version=exp_version,
            )
            alerter.schema_drift_alert(
                config.pipeline_name, source_name, run_id,
                drift.summary(), drift.root_cause_hints(),
                healed=False,
                metrics={"batch_index": batch_idx, "rows": len(batch)},
            )
            result.rows_quarantined += len(batch)
            bus.emit(
                run_id=run_id,
                pipeline_name=config.pipeline_name,
                event_type="QuarantineTriggered",
                severity="ERROR",
                component="quarantine",
                message=f"Strict mode quarantined {len(batch)} rows",
                trace_id=trace_id,
                span_id=new_span_id("quarantine"),
                metadata={"rows_quarantined": len(batch), "error_type": "SCHEMA_DRIFT"},
            )
            continue

        # --- Attempt healing ---
        heal = healer.heal(batch, drift)
        result.healing_results.append(heal)

        if not heal.success:
            _quarantine_batch(
                quarantine, batch, config, source_name, run_id,
                "HEALING_FAILED", heal.failure_reason,
                root_cause_hint="\n".join(drift.root_cause_hints()),
                schema_version=exp_version,
            )
            alerter.schema_drift_alert(
                config.pipeline_name, source_name, run_id,
                drift.summary(), drift.root_cause_hints(),
                healed=False,
                metrics={"batch_index": batch_idx, "rows": len(batch), "coercion_loss": heal.rows_coerced},
            )
            result.rows_quarantined += len(batch)
            bus.emit(
                run_id=run_id,
                pipeline_name=config.pipeline_name,
                event_type="QuarantineTriggered",
                severity="ERROR",
                component="quarantine",
                message=f"Healing failed; quarantined {len(batch)} rows",
                trace_id=trace_id,
                span_id=new_span_id("quarantine"),
                metadata={"rows_quarantined": len(batch), "error_type": "HEALING_FAILED", "reason": heal.failure_reason},
            )
            continue

        # Healing succeeded — quarantine any coercion failures within the batch
        if heal.failed_records:
            quarantine.quarantine_records(
                heal.failed_records, config.pipeline_name, source_name, run_id,
                error_type="COERCION_FAILURE",
                error_detail="Individual row coercion failed during healing",
                root_cause_hint="\n".join(drift.root_cause_hints()),
                schema_version=exp_version,
            )
            result.rows_quarantined += len(heal.failed_records)
            bus.emit(
                run_id=run_id,
                pipeline_name=config.pipeline_name,
                event_type="QuarantineTriggered",
                severity="ERROR",
                component="quarantine",
                message=f"Quarantined {len(heal.failed_records)} coercion-failed rows",
                trace_id=trace_id,
                span_id=new_span_id("quarantine"),
                metadata={"rows_quarantined": len(heal.failed_records), "error_type": "COERCION_FAILURE"},
            )
            alerter.quarantine_alert(
                config.pipeline_name, source_name, run_id,
                len(heal.failed_records), "COERCION_FAILURE",
                drift.root_cause_hints(),
                metrics={"batch_index": batch_idx},
            )

        # If schema evolution is on, register the new schema
        if config.healing.enable_schema_evolution and (
            drift.added_columns or drift.type_changes
        ):
            new_version = registry.register(source_name, schema_from_df(heal.healed_df))
            schema_info = registry.get_active(source_name)
            result.schema_evolved = True
            quarantine.log_drift_event(
                pipeline_name=config.pipeline_name,
                source_name=source_name,
                run_id=run_id,
                drift_type=",".join(drift.drift_types),
                details_json=drift.to_details_json(),
                healed=True,
                healing_action=f"Schema evolved to v{new_version}. Actions: {heal.summary()}",
            )
            logger.info("Schema evolved to v%d for source '%s'", new_version, source_name)

        alerter.schema_drift_alert(
            config.pipeline_name, source_name, run_id,
            drift.summary(), drift.root_cause_hints(),
            healed=True,
            metrics={
                "batch_index": batch_idx,
                "rows_healed": len(heal.healed_df),
                "rows_coerced": heal.rows_coerced,
                "actions": heal.summary(),
            },
        )

        result.clean_batches.append(heal.healed_df)
        result.rows_clean += len(heal.healed_df)

    logger.info(
        "Transform complete: total=%d clean=%d quarantined=%d schema_evolved=%s",
        result.rows_total, result.rows_clean, result.rows_quarantined, result.schema_evolved,
    )
    bus.emit(
        run_id=run_id,
        pipeline_name=config.pipeline_name,
        event_type="TransformCompleted",
        severity="INFO",
        component="transformer",
        message=f"Transform complete: clean={result.rows_clean}, quarantined={result.rows_quarantined}",
        trace_id=trace_id,
        span_id=new_span_id("transform-completed"),
        metadata={
            "rows_total": result.rows_total,
            "rows_clean": result.rows_clean,
            "rows_quarantined": result.rows_quarantined,
            "schema_evolved": result.schema_evolved,
        },
    )
    return result


def _quarantine_batch(
    store: QuarantineStore,
    batch: pd.DataFrame,
    config: ETLConfig,
    source_name: str,
    run_id: str,
    error_type: str,
    error_detail: str,
    root_cause_hint: str = "",
    schema_version: int | None = None,
) -> None:
    records = batch.to_dict(orient="records")
    store.quarantine_records(
        records, config.pipeline_name, source_name, run_id,
        error_type=error_type,
        error_detail=error_detail,
        root_cause_hint=root_cause_hint,
        schema_version=schema_version,
    )
