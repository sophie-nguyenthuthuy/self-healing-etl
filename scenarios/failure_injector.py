from __future__ import annotations

import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from agents.autonomous_loop import AutonomousHealingLoop
from config import AutonomousConfig, ETLConfig, HealingConfig, QuarantineConfig, SchemaRegistryConfig
from observability.event_bus import EventBus
from observability.traces import new_trace_id
from quarantine.store import QuarantineStore
from scenarios.k8s_failure_injector import K8sFailureInjector, InjectionResult as K8sInjectionResult


@dataclass(frozen=True)
class FailureInjectionResult:
    scenario: str
    domain: str
    status: str
    message: str
    mttr_seconds: float
    db_url: str
    evidence: dict[str, Any] = field(default_factory=dict)


class FailureInjector:
    """Unified injector for real K8s failures and local ETL staging failures."""

    K8S_SCENARIOS = {"crash_loop", "image_pull", "oom", "scale_zero"}
    ETL_SCENARIOS = {"api_rate_limit", "staging_data_quality", "timeout", "concurrent_modification"}

    def __init__(
        self,
        *,
        db_url: str | None = None,
        namespace: str = "self-healing-etl",
        deployment: str = "self-healing-etl",
        apps_v1: Any | None = None,
    ):
        self.root = Path(tempfile.gettempdir()) / f"self_healing_unified_demo_{uuid.uuid4().hex}"
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_url = db_url or f"sqlite:///{(self.root / 'observability.db').as_posix()}"
        self.namespace = namespace
        self.deployment = deployment
        self.k8s = K8sFailureInjector(namespace=namespace, deployment=deployment, apps_v1=apps_v1)

    def inject(self, scenario: str) -> FailureInjectionResult | K8sInjectionResult:
        scenario = scenario.replace("-", "_").lower()
        if scenario in self.K8S_SCENARIOS:
            return self.k8s.inject(scenario)
        if scenario == "api_rate_limit":
            return self.inject_api_rate_limit()
        if scenario == "staging_data_quality":
            return self.inject_staging_data_quality()
        if scenario == "timeout":
            return self.inject_timeout()
        if scenario == "concurrent_modification":
            return self.inject_concurrent_modification()
        raise ValueError(f"Unsupported failure scenario: {scenario}")

    def inject_api_rate_limit(self) -> FailureInjectionResult:
        return self._autonomous_event_scenario(
            scenario="api_rate_limit",
            event_type="ExtractFailed",
            component="extractor",
            message="HTTP 429 Too Many Requests from SaaS source API",
            metadata={"http_status": 429, "source_system": "salesforce", "retry_after_seconds": 2},
            context={"source_type": "api", "summary": {"rows_loaded": 1, "rows_quarantined": 0}},
        )

    def inject_timeout(self) -> FailureInjectionResult:
        return self._autonomous_event_scenario(
            scenario="timeout",
            event_type="LoadFailed",
            component="loader",
            message="Destination unreachable timeout while writing staging batch",
            metadata={"platform_pattern": "fabric_lakehouse", "timeout_seconds": 30},
            context={"destination_type": "lakehouse", "destination_reachable": True, "summary": {"rows_loaded": 1, "rows_quarantined": 0}},
        )

    def inject_concurrent_modification(self) -> FailureInjectionResult:
        return self._autonomous_event_scenario(
            scenario="concurrent_modification",
            event_type="LoadFailed",
            component="loader",
            message="Concurrent modification detected during Delta staging write",
            metadata={"platform_pattern": "databricks_delta", "conflict": "concurrent modification"},
            context={"destination_type": "delta", "summary": {"rows_loaded": 1, "rows_quarantined": 0}},
        )

    def inject_staging_data_quality(self) -> FailureInjectionResult:
        started = time.monotonic()
        cfg = self._etl_config("staging_data_quality")
        source_name = "taxi_staging_quality"
        raw = pd.DataFrame(
            [
                {"trip_id": "trip-1", "fare_amount": 25.0, "pickup_datetime": "2026-06-10T09:00:00"},
                {"trip_id": "trip-1", "fare_amount": 18.0, "pickup_datetime": "2026-06-10T09:05:00"},
                {"trip_id": None, "fare_amount": 11.0, "pickup_datetime": "2026-06-10T09:10:00"},
                {"trip_id": "trip-4", "fare_amount": 33.0, "pickup_datetime": "2026-06-10T09:15:00"},
            ]
        )
        violations = raw["trip_id"].isna() | raw["trip_id"].duplicated(keep=False)
        bad_rows = raw[violations].to_dict(orient="records")
        clean = raw[~violations].copy()
        trace_id = new_trace_id()
        run_id = f"staging-dq-{uuid.uuid4().hex[:8]}"
        bus = EventBus(cfg.quarantine.db_url)
        store = QuarantineStore(cfg.quarantine.db_url)
        bus.emit(
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_type="StagingDataQualityViolation",
            severity="ERROR",
            component="staging_quality",
            message="Unexpected nulls or duplicate keys detected in staging batch",
            trace_id=trace_id,
            metadata={"violating_rows": len(bad_rows), "rules": ["not_null:trip_id", "unique:trip_id"]},
        )
        quarantined = store.quarantine_records(
            bad_rows,
            pipeline_name=cfg.pipeline_name,
            source_name=source_name,
            run_id=run_id,
            error_type="STAGING_DATA_QUALITY",
            error_detail="not_null/unique constraint violation for trip_id",
            root_cause_hint="dbt-style staging tests failed; violating rows isolated for review",
        )
        rows_loaded = len(clean)
        bus.emit(
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_type="StagingCleanRowsLoaded",
            severity="INFO",
            component="staging_quality",
            message="Clean staging rows continued after constraint isolation",
            trace_id=trace_id,
            metadata={"rows_loaded": rows_loaded},
        )
        loop = AutonomousHealingLoop(cfg.quarantine.db_url, cfg.autonomous)
        event = bus.emit(
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_type="QuarantineTriggered",
            severity="WARNING",
            component="staging_quality",
            message="Constraint violation rows quarantined; clean rows can proceed",
            trace_id=trace_id,
            metadata={"rows_quarantined": quarantined, "rows_loaded": rows_loaded},
        )
        loop_result = loop.handle_failure(
            event,
            context={"summary": {"rows_loaded": rows_loaded, "rows_quarantined": quarantined}, "max_quarantine_rows": quarantined},
        )
        return FailureInjectionResult(
            scenario="staging_data_quality",
            domain="ETL",
            status="RECOVERED",
            message="Quarantined violating staging rows and loaded clean rows.",
            mttr_seconds=time.monotonic() - started,
            db_url=cfg.quarantine.db_url,
            evidence={
                "rows_loaded": rows_loaded,
                "rows_quarantined": quarantined,
                "autonomous_success": loop_result.success,
            },
        )

    def _autonomous_event_scenario(
        self,
        *,
        scenario: str,
        event_type: str,
        component: str,
        message: str,
        metadata: dict[str, Any],
        context: dict[str, Any],
    ) -> FailureInjectionResult:
        started = time.monotonic()
        cfg = self._etl_config(scenario)
        trace_id = new_trace_id()
        run_id = f"{scenario}-{uuid.uuid4().hex[:8]}"
        bus = EventBus(cfg.quarantine.db_url)
        event = bus.emit(
            run_id=run_id,
            pipeline_name=cfg.pipeline_name,
            event_type=event_type,
            severity="ERROR",
            component=component,
            message=message,
            trace_id=trace_id,
            metadata=metadata,
        )
        result = AutonomousHealingLoop(cfg.quarantine.db_url, cfg.autonomous).handle_failure(event, context=context)
        status = "RECOVERED" if result.success else "ESCALATED"
        return FailureInjectionResult(
            scenario=scenario,
            domain="ETL",
            status=status,
            message=message,
            mttr_seconds=time.monotonic() - started,
            db_url=cfg.quarantine.db_url,
            evidence={
                "root_cause": result.rca.root_cause,
                "actions": [action.action_type for action in result.actions],
                "escalated": result.escalated,
            },
        )

    def _etl_config(self, scenario: str) -> ETLConfig:
        return ETLConfig(
            pipeline_name=f"etl_demo_{scenario}",
            batch_size=100,
            schema_registry=SchemaRegistryConfig(db_url=f"sqlite:///{(self.root / f'{scenario}_schema.db').as_posix()}"),
            quarantine=QuarantineConfig(db_url=self.db_url),
            healing=HealingConfig(max_coercion_loss_pct=20),
            autonomous=AutonomousConfig(enable_autonomous_healing=True, ollama_enabled=False, max_healing_attempts=1),
        )
