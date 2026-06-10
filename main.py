"""
CLI entry point for the self-healing ETL framework.

Usage examples:
    # Run a CSV file through the pipeline
    python main.py --source orders.csv --dest output.csv

    # Run with strict schema mode (quarantine on any drift, no healing)
    python main.py --source orders.csv --dest output.csv --strict

    # Run the interactive demo
    python main.py --demo

    # Run the real-time taxi ETL simulation
    python main.py --taxi-demo --taxi-records 30

    # Run executable failure/healing scenario coverage
    python main.py --failure-scenarios
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
import uuid
from pathlib import Path

# Disable Prefect telemetry to prevent SQLite concurrent lock errors during async execution
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
os.environ["DO_NOT_TRACK"] = "1"
_prefect_home = Path(tempfile.gettempdir()) / f"self_healing_prefect_home_{uuid.uuid4().hex}"
_prefect_home.mkdir(parents=True, exist_ok=True)
os.environ["PREFECT_HOME"] = str(_prefect_home)
os.environ["PREFECT_PROFILES_PATH"] = str(_prefect_home / "profiles.toml")
os.environ["PREFECT_RESULTS_PERSIST_BY_DEFAULT"] = "false"
os.environ["PREFECT_SERVER_MEMOIZE_BLOCK_AUTO_REGISTRATION"] = "false"
os.environ["PREFECT_SERVER_MEMO_STORE_PATH"] = str(_prefect_home / "memo_store.toml")
os.environ["PREFECT_SERVER_SERVICES_TASK_RUN_RECORDER_ENABLED"] = "false"
_prefect_db = Path(tempfile.gettempdir()) / f"self_healing_prefect_{uuid.uuid4().hex}.db"
os.environ["PREFECT_SERVER_DATABASE_CONNECTION_URL"] = f"sqlite+aiosqlite:///{_prefect_db.as_posix()}"

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from config import ETLConfig, SchemaRegistryConfig, QuarantineConfig, HealingConfig, AlertConfig
from config import AutonomousConfig
from pipeline.orchestrator import etl_flow
from quarantine.store import QuarantineStore


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="etl",
        description="Self-Healing ETL — Prefect pipeline with schema drift detection",
    )
    p.add_argument("--demo", action="store_true", help="Run the built-in demo scenario")
    p.add_argument("--failure-scenarios", action="store_true",
                   help="Run executable ETL failure and auto-healing scenario coverage")
    p.add_argument("--autonomous-mode", action="store_true",
                   help="Enable agentic observability, RCA, planning, healing audit, and validation")
    p.add_argument("--human-approval", action="store_true",
                   help="In autonomous mode, generate plans but persist them for approval before execution")
    p.add_argument("--include-hard-failures", action="store_true",
                   help="Also run intentional source/load hard-failure cases with --failure-scenarios")
    p.add_argument("--taxi-demo", action="store_true",
                   help="Run the real-time taxi ETL simulation once")
    p.add_argument("--taxi-stream", action="store_true",
                   help="Continuously watch data/taxi/incoming for taxi micro-batches")
    p.add_argument("--taxi-records", type=int, default=30,
                   help="Number of synthetic/source taxi records to produce for --taxi-demo")
    p.add_argument("--taxi-batch-size", type=int, default=5,
                   help="Records per incoming micro-batch for --taxi-demo")
    p.add_argument("--taxi-source-csv", default=None,
                   help="Optional NYC TLC CSV file to stream instead of synthetic records")
    p.add_argument("--warehouse-db", default=None,
                   help="Warehouse SQLAlchemy URL for taxi ETL; defaults to data/taxi SQLite")
    p.add_argument("--taxi-root", default=None,
                   help="Runtime folder for taxi incoming/processed DB files; defaults to data/taxi")
    p.add_argument("--taxi-no-drift", action="store_true",
                   help="Disable demo drift injection for taxi producer records")
    p.add_argument("--taxi-drift-after", type=int, default=None,
                   help="Record number after which taxi demo drift injection starts")
    p.add_argument("--taxi-random-failures", action="store_true",
                   help="Inject random taxi schema/type/data-quality failures for autonomous discovery")
    p.add_argument("--source", help="Source file path (CSV or JSONL)")
    p.add_argument("--source-type", default="csv", choices=["csv", "jsonl"],
                   help="Source format (default: csv)")
    p.add_argument("--dest", help="Destination file path")
    p.add_argument("--dest-type", default="csv", choices=["csv", "jsonl", "db", "memory"],
                   help="Destination format (default: csv)")
    p.add_argument("--source-name", default="source", help="Logical source name (used in registry)")
    p.add_argument("--pipeline-name", default="etl_pipeline")
    p.add_argument("--strict", action="store_true",
                   help="Strict schema mode: quarantine any drift rather than healing")
    p.add_argument("--no-coercion", action="store_true", help="Disable type coercion")
    p.add_argument("--no-backfill", action="store_true", help="Disable column backfill")
    p.add_argument("--no-evolution", action="store_true", help="Disable schema evolution")
    p.add_argument("--slack-webhook", default=None, help="Slack webhook URL for alerts")
    p.add_argument("--verbose", "-v", action="store_true")
    return p


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    if args.demo:
        from demo import main as demo_main
        demo_main()
        return

    if args.failure_scenarios:
        from scenarios.failure_scenarios import run_failure_scenarios
        run_failure_scenarios(include_hard_failures=args.include_hard_failures)
        return

    if args.taxi_demo:
        from taxi_etl.realtime import run_taxi_demo
        run_taxi_demo(
            records=args.taxi_records,
            batch_size=args.taxi_batch_size,
            source_csv=args.taxi_source_csv,
            warehouse_url=args.warehouse_db,
            taxi_root=args.taxi_root,
            inject_drift=not args.taxi_no_drift,
            drift_start_after=args.taxi_drift_after,
            autonomous_mode=args.autonomous_mode,
            require_human_approval=args.human_approval,
            random_failures=args.taxi_random_failures,
        )
        return

    if args.taxi_stream:
        from taxi_etl.realtime import run_realtime_etl
        print("Watching data/taxi/incoming for taxi micro-batches. Press Ctrl+C to stop.")
        run_realtime_etl(
            warehouse_url=args.warehouse_db,
            taxi_root=args.taxi_root,
            autonomous_mode=args.autonomous_mode,
            require_human_approval=args.human_approval,
        )
        return

    if not args.source:
        print("Error: --source is required (or use --demo, --failure-scenarios, --taxi-demo, or --taxi-stream)")
        sys.exit(1)

    cfg = ETLConfig(
        pipeline_name=args.pipeline_name,
        schema_registry=SchemaRegistryConfig(strict_mode=args.strict),
        healing=HealingConfig(
            enable_type_coercion=not args.no_coercion,
            enable_column_backfill=not args.no_backfill,
            enable_schema_evolution=not args.no_evolution,
        ),
        autonomous=AutonomousConfig(
            enable_autonomous_healing=args.autonomous_mode,
            require_human_approval=args.human_approval,
        ),
        alerts=AlertConfig(slack_webhook_url=args.slack_webhook),
    )

    summary = etl_flow(
        source_name=args.source_name,
        source_type=args.source_type,
        destination_type=args.dest_type,
        config=cfg,
        source_path=args.source,
        destination_path=args.dest,
    )

    print("\nPipeline completed:")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    stats = QuarantineStore(cfg.quarantine.db_url).stats(pipeline_name=cfg.pipeline_name)
    print("\nEvaluation metrics:")
    print(f"  MTTD (pipeline detection latency): {_format_seconds(stats['mttd_seconds'])}")
    print(f"  MTTR (manual quarantine resolution): {_format_seconds(stats['mttr_seconds'])}")


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.3f}s"


if __name__ == "__main__":
    main()
