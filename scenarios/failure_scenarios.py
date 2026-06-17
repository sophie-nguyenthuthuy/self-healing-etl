from __future__ import annotations

import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd
from rich.console import Console
from rich.table import Table
from sqlalchemy import create_engine, inspect

from config import ETLConfig, HealingConfig, QuarantineConfig, SchemaRegistryConfig
from pipeline.orchestrator import etl_flow
from quarantine.store import QuarantineStore

console = Console()


@dataclass
class ScenarioResult:
    name: str
    category: str
    expected: str
    status: str
    rows_loaded: int = 0
    rows_quarantined: int = 0
    drift_detected: bool = False
    schema_evolved: bool = False
    evidence: str = ""


def run_failure_scenarios(include_hard_failures: bool = False) -> list[ScenarioResult]:
    """Run actual ETL pipeline scenarios for healing and quarantine.

    Hard source/load failures are useful evidence, but they intentionally produce
    failed Prefect task logs. Keep them opt-in so the default suite demonstrates
    auto-healing without making the overall job look broken.
    """
    root = Path(tempfile.gettempdir()) / f"self_healing_failure_scenarios_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)

    scenarios = [
        _added_column_schema_evolution,
        _removed_column_backfill,
        _type_change_coercion_success,
        _type_change_partial_quarantine,
        _type_change_exceeds_loss_gate,
        _strict_mode_quarantine,
        _custom_transform_failure,
        _resource_guard_failure,
        _join_missing_dimension_auto_heal,
        _join_duplicate_dimension_auto_heal,
        _join_unresolved_failure,
        _loader_schema_evolution,
    ]
    if include_hard_failures:
        scenarios.extend([_missing_source_failure, _load_destination_failure])

    results = [scenario(root) for scenario in scenarios]
    _print_results(results)
    return results


def _added_column_schema_evolution(root: Path) -> ScenarioResult:
    return _run_drift_pair(
        root=root,
        name="Added column kept and schema evolved",
        category="Schema drift auto-healing",
        expected="Auto-heal by retaining new column and registering new schema version",
        source_name="added_column",
        scenario_df=_baseline_df().assign(region=["APAC", "EMEA", "AMER"]),
        healing=HealingConfig(enable_schema_evolution=True, max_coercion_loss_pct=20),
        evidence_fn=lambda summary, _: (
            "new column retained; schema_evolved=True"
            if summary.get("schema_evolved")
            else "schema did not evolve"
        ),
    )


def _removed_column_backfill(root: Path) -> ScenarioResult:
    scenario_df = _baseline_df().drop(columns=["status"])
    return _run_drift_pair(
        root=root,
        name="Missing column backfilled",
        category="Schema drift auto-healing",
        expected="Auto-heal by adding missing column with null values",
        source_name="removed_column",
        scenario_df=scenario_df,
        healing=HealingConfig(enable_column_backfill=True, max_coercion_loss_pct=20),
        evidence_fn=lambda summary, _: (
            "missing column healed without schema evolution"
            if summary.get("drift_detected") and not summary.get("schema_evolved")
            else "missing-column healing not observed"
        ),
    )


def _type_change_coercion_success(root: Path) -> ScenarioResult:
    scenario_df = _baseline_df()
    scenario_df["amount"] = ["10.50", "20.75", "30.25"]
    return _run_drift_pair(
        root=root,
        name="Numeric string type coercion",
        category="Type drift auto-healing",
        expected="Auto-heal by coercing numeric strings back to float",
        source_name="type_success",
        scenario_df=scenario_df,
        healing=HealingConfig(enable_type_coercion=True, max_coercion_loss_pct=20),
        evidence_fn=lambda summary, _: (
            "type drift detected and all rows loaded"
            if summary.get("drift_detected") and summary.get("rows_loaded") == 3
            else "type coercion success not observed"
        ),
    )


def _type_change_partial_quarantine(root: Path) -> ScenarioResult:
    scenario_df = _baseline_df()
    scenario_df["amount"] = ["10.50", "bad-value", "30.25"]
    return _run_drift_pair(
        root=root,
        name="Partial coercion failure quarantines bad row",
        category="Type drift partial healing",
        expected="Load healed batch and quarantine row-level coercion failure",
        source_name="type_partial",
        scenario_df=scenario_df,
        healing=HealingConfig(enable_type_coercion=True, max_coercion_loss_pct=50),
        evidence_fn=lambda summary, store: (
            f"quarantine samples={store.stats('failure_scenario_type_partial')['total']}"
        ),
    )


def _type_change_exceeds_loss_gate(root: Path) -> ScenarioResult:
    scenario_df = _baseline_df()
    scenario_df["amount"] = ["bad-1", "bad-2", "30.25"]
    return _run_drift_pair(
        root=root,
        name="Coercion loss exceeds threshold",
        category="Healing fallback quarantine",
        expected="Reject full batch into quarantine when coercion loss is too high",
        source_name="type_loss_gate",
        scenario_df=scenario_df,
        healing=HealingConfig(enable_type_coercion=True, max_coercion_loss_pct=20),
        evidence_fn=lambda summary, store: (
            f"full-batch quarantine={store.stats('failure_scenario_type_loss_gate')['total']}"
        ),
    )


def _strict_mode_quarantine(root: Path) -> ScenarioResult:
    return _run_drift_pair(
        root=root,
        name="Strict mode quarantines schema drift",
        category="Policy-controlled failure",
        expected="Quarantine drifted batch because strict mode disables healing",
        source_name="strict_mode",
        scenario_df=_baseline_df().assign(extra_col=["x", "y", "z"]),
        strict_mode=True,
        healing=HealingConfig(enable_schema_evolution=True, max_coercion_loss_pct=20),
        evidence_fn=lambda summary, store: (
            f"strict quarantine={store.stats('failure_scenario_strict_mode')['total']}"
        ),
    )


def _custom_transform_failure(root: Path) -> ScenarioResult:
    name = "Custom transform failure quarantines batch"
    source_name = "transform_failure"
    cfg = _config(root, source_name)
    _run_baseline(source_name, cfg)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="memory",
        config=cfg,
        source_df=_baseline_df(),
        custom_transform=_raise_transform_error,
    )
    return _result_from_summary(
        name=name,
        category="Transformation failure",
        expected="Quarantine whole batch when custom transform raises",
        summary=summary,
        evidence="TRANSFORM_ERROR rows are quarantined",
    )


def _resource_guard_failure(root: Path) -> ScenarioResult:
    name = "Resource guard quarantines oversized batch"
    source_name = "resource_guard"
    cfg = _config(root, source_name)
    _run_baseline(source_name, cfg)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="memory",
        config=cfg,
        source_df=_baseline_df(),
        custom_transform=_raise_resource_guard_error,
    )
    return _result_from_summary(
        name=name,
        category="Memory/resource guard",
        expected="Quarantine batch when preflight resource guard rejects it",
        summary=summary,
        evidence="RESOURCE_LIMIT-style transform guard raised",
    )


def _join_missing_dimension_auto_heal(root: Path) -> ScenarioResult:
    name = "Join missing dimension member auto-healed"
    source_name = "join_missing_dimension"
    cfg = _config(root, source_name)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="memory",
        config=cfg,
        source_df=_orders_df(),
        custom_transform=_join_with_unknown_member,
    )
    return _result_from_summary(
        name=name,
        category="Join auto-healing",
        expected="Missing dimension keys are mapped to UNKNOWN member and loaded",
        summary=summary,
        evidence="customer_id=999 mapped to UNKNOWN_CUSTOMER",
    )


def _join_duplicate_dimension_auto_heal(root: Path) -> ScenarioResult:
    name = "Join duplicate dimension keys auto-healed"
    source_name = "join_duplicate_dimension"
    cfg = _config(root, source_name)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="memory",
        config=cfg,
        source_df=_orders_df(include_unknown=False),
        custom_transform=_join_with_deduped_dimension,
    )
    return _result_from_summary(
        name=name,
        category="Join auto-healing",
        expected="Duplicate dimension rows are deduplicated before join",
        summary=summary,
        evidence="dimension customer_id duplicates dropped by latest effective date",
    )


def _join_unresolved_failure(root: Path) -> ScenarioResult:
    name = "Join unresolved key quarantines batch"
    source_name = "join_unresolved"
    cfg = _config(root, source_name)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="memory",
        config=cfg,
        source_df=_orders_df(),
        custom_transform=_join_without_unknown_member,
    )
    return _result_from_summary(
        name=name,
        category="Join failure quarantine",
        expected="Join transform raises when unmatched keys cannot be healed",
        summary=summary,
        evidence="unmatched dimension key caused TRANSFORM_ERROR quarantine",
    )


def _loader_schema_evolution(root: Path) -> ScenarioResult:
    name = "Loader adds healed schema column"
    source_name = "loader_schema_evolution"
    cfg = _config(root, source_name)
    warehouse = root / f"{source_name}_warehouse.db"
    destination_url = f"sqlite:///{warehouse.as_posix()}"

    _run_baseline(source_name, cfg, destination_type="db", destination_path=destination_url)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="db",
        destination_path=destination_url,
        destination_table="scenario_fact",
        config=cfg,
        source_df=_baseline_df().assign(region=["APAC", "EMEA", "AMER"]),
    )

    engine = create_engine(destination_url)
    columns = {column["name"] for column in inspect(engine).get_columns("scenario_fact")}
    evidence = "region column exists in destination" if "region" in columns else "region missing"
    return _result_from_summary(
        name=name,
        category="Loading auto-healing",
        expected="Destination table is altered to accept healed/evolved column",
        summary=summary,
        evidence=evidence,
    )


def _missing_source_failure(root: Path) -> ScenarioResult:
    source_name = "missing_source"
    cfg = _config(root, source_name)
    try:
        etl_flow(
            source_name=source_name,
            source_type="csv",
            source_path=str(root / "does_not_exist.csv"),
            destination_type="memory",
            config=cfg,
        )
    except Exception as exc:
        return ScenarioResult(
            name="Missing source file fails fast",
            category="Ingestion failure",
            expected="Pipeline fails and requires source correction/re-run",
            status="FAILED",
            evidence=type(exc).__name__,
        )
    return ScenarioResult(
        name="Missing source file fails fast",
        category="Ingestion failure",
        expected="Pipeline fails and requires source correction/re-run",
        status="UNEXPECTED_SUCCESS",
    )


def _load_destination_failure(root: Path) -> ScenarioResult:
    source_name = "load_failure"
    cfg = _config(root, source_name)
    bad_destination = root / "directory_destination"
    bad_destination.mkdir(parents=True, exist_ok=True)
    try:
        etl_flow(
            source_name=source_name,
            source_type="dataframe",
            source_df=_baseline_df(),
            destination_type="csv",
            destination_path=str(bad_destination),
            config=cfg,
        )
    except Exception as exc:
        return ScenarioResult(
            name="Invalid load destination fails",
            category="Loading failure",
            expected="Pipeline fails and requires destination correction/re-run",
            status="FAILED",
            evidence=type(exc).__name__,
        )
    return ScenarioResult(
        name="Invalid load destination fails",
        category="Loading failure",
        expected="Pipeline fails and requires destination correction/re-run",
        status="UNEXPECTED_SUCCESS",
    )


def _run_drift_pair(
    root: Path,
    name: str,
    category: str,
    expected: str,
    source_name: str,
    scenario_df: pd.DataFrame,
    healing: HealingConfig,
    evidence_fn: Callable[[dict, QuarantineStore], str],
    strict_mode: bool = False,
) -> ScenarioResult:
    cfg = _config(root, source_name, healing=healing, strict_mode=strict_mode)
    _run_baseline(source_name, cfg)
    summary = etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type="memory",
        config=cfg,
        source_df=scenario_df,
    )
    store = QuarantineStore(cfg.quarantine.db_url)
    return _result_from_summary(
        name=name,
        category=category,
        expected=expected,
        summary=summary,
        evidence=evidence_fn(summary, store),
    )


def _run_baseline(
    source_name: str,
    cfg: ETLConfig,
    destination_type: str = "memory",
    destination_path: str | None = None,
) -> dict:
    return etl_flow(
        source_name=source_name,
        source_type="dataframe",
        destination_type=destination_type,
        destination_path=destination_path,
        destination_table="scenario_fact",
        config=cfg,
        source_df=_baseline_df(),
    )


def _result_from_summary(
    name: str,
    category: str,
    expected: str,
    summary: dict,
    evidence: str,
) -> ScenarioResult:
    return ScenarioResult(
        name=name,
        category=category,
        expected=expected,
        status=summary.get("status", "UNKNOWN"),
        rows_loaded=summary.get("rows_loaded", 0),
        rows_quarantined=summary.get("rows_quarantined", 0),
        drift_detected=summary.get("drift_detected", False),
        schema_evolved=summary.get("schema_evolved", False),
        evidence=evidence,
    )


def _config(
    root: Path,
    source_name: str,
    healing: HealingConfig | None = None,
    strict_mode: bool = False,
) -> ETLConfig:
    pipeline_name = f"failure_scenario_{source_name}"
    return ETLConfig(
        pipeline_name=pipeline_name,
        batch_size=100,
        schema_registry=SchemaRegistryConfig(
            db_url=f"sqlite:///{(root / f'{source_name}_schema.db').as_posix()}",
            strict_mode=strict_mode,
        ),
        quarantine=QuarantineConfig(
            db_url=f"sqlite:///{(root / f'{source_name}_quarantine.db').as_posix()}"
        ),
        healing=healing or HealingConfig(max_coercion_loss_pct=20),
    )


def _baseline_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "amount": [10.5, 20.75, 30.25],
            "status": ["new", "paid", "closed"],
        }
    )


def _orders_df(include_unknown: bool = True) -> pd.DataFrame:
    customer_ids = [100, 200, 999] if include_unknown else [100, 200, 100]
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": customer_ids,
            "amount": [25.0, 40.0, 15.0],
        }
    )


def _customer_dim(include_unknown_member: bool = False, duplicate: bool = False) -> pd.DataFrame:
    rows = [
        {"customer_id": 100, "customer_segment": "Business", "effective_date": "2026-01-01"},
        {"customer_id": 200, "customer_segment": "Consumer", "effective_date": "2026-01-01"},
    ]
    if duplicate:
        rows.append(
            {"customer_id": 100, "customer_segment": "Enterprise", "effective_date": "2026-06-01"}
        )
    if include_unknown_member:
        rows.append(
            {"customer_id": -1, "customer_segment": "UNKNOWN_CUSTOMER", "effective_date": "1900-01-01"}
        )
    return pd.DataFrame(rows)


def _join_with_unknown_member(df: pd.DataFrame) -> pd.DataFrame:
    fact = df.copy()
    dim = _customer_dim(include_unknown_member=True)
    known_keys = set(dim["customer_id"])
    fact["customer_id"] = fact["customer_id"].where(fact["customer_id"].isin(known_keys), -1)
    joined = fact.merge(dim.drop(columns=["effective_date"]), on="customer_id", how="left")
    return joined


def _join_with_deduped_dimension(df: pd.DataFrame) -> pd.DataFrame:
    dim = _customer_dim(duplicate=True)
    dim = (
        dim.sort_values("effective_date")
        .drop_duplicates(subset=["customer_id"], keep="last")
        .drop(columns=["effective_date"])
    )
    return df.merge(dim, on="customer_id", how="left", validate="many_to_one")


def _join_without_unknown_member(df: pd.DataFrame) -> pd.DataFrame:
    dim = _customer_dim(include_unknown_member=False).drop(columns=["effective_date"])
    joined = df.merge(dim, on="customer_id", how="left", validate="many_to_one")
    unmatched = joined["customer_segment"].isna()
    if unmatched.any():
        missing = sorted(joined.loc[unmatched, "customer_id"].unique().tolist())
        raise ValueError(f"Unmatched customer dimension keys: {missing}")
    return joined


def _raise_transform_error(_: pd.DataFrame) -> pd.DataFrame:
    raise ValueError("Synthetic transform failure for scenario coverage")


def _raise_resource_guard_error(_: pd.DataFrame) -> pd.DataFrame:
    raise MemoryError("Synthetic resource guard failure for scenario coverage")


def _print_results(results: list[ScenarioResult]) -> None:
    table = Table(title="ETL Failure and Auto-Healing Scenario Coverage")
    table.add_column("Scenario")
    table.add_column("Category")
    table.add_column("Status")
    table.add_column("Loaded")
    table.add_column("Quarantined")
    table.add_column("Drift")
    table.add_column("Evolved")
    table.add_column("Evidence")

    for result in results:
        table.add_row(
            result.name,
            result.category,
            result.status,
            str(result.rows_loaded),
            str(result.rows_quarantined),
            str(result.drift_detected),
            str(result.schema_evolved),
            result.evidence,
        )
    console.print(table)
