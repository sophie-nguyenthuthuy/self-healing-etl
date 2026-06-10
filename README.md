# Self-Healing ETL Framework

A Prefect-based ETL pipeline orchestrator that automatically detects schema drift, quarantines bad records, applies healing strategies, and sends structured alerts with root-cause hints.

## Features

- **Schema drift detection** — compares every incoming batch against a versioned schema registry; flags added columns, removed columns, and type changes
- **Auto-healing** — backfills missing columns, coerces mismatched types, and retains new columns for schema evolution
- **Quarantine store** — bad records are isolated with full metadata (error type, root-cause hint, originating run) rather than dropped silently
- **Structured alerts** — console and Slack alerts include a summary, root-cause hints, and suggested remediation steps
- **Schema versioning** — successful healing automatically registers a new schema version so subsequent runs converge cleanly
- **Prefect orchestration** — each stage (extract, transform, load) is a retryable Prefect task; the full pipeline is a tracked flow

## Project Structure

```
etl_framework/
├── config.py                 # Pydantic config dataclasses
├── models.py                 # SQLAlchemy ORM models
├── schema/
│   ├── registry.py           # Schema versioning (register, get_active, history)
│   └── drift_detector.py     # DriftDetector → DriftReport
├── quarantine/
│   └── store.py              # Quarantine records + drift events; stats; purge
├── healing/
│   └── strategies.py         # HealingEngine (backfill, coercion, evolution)
├── alerts/
│   └── alerter.py            # Multi-channel alerter with root-cause hints
├── pipeline/
│   ├── extractor.py          # Prefect task: CSV / JSONL / DataFrame sources
│   ├── transformer.py        # Prefect task: drift detection + healing
│   ├── loader.py             # Prefect task: CSV / JSONL / DB / memory sinks
│   └── orchestrator.py       # @flow: wires extract → transform → load
├── demo.py                   # 3-run scenario demonstrating full lifecycle
├── main.py                   # CLI entry point
└── requirements.txt
```

## Quick Start

```bash
git clone https://github.com/sophie-nguyenthuthuy/self-healing-etl.git
cd self-healing-etl
pip install -r requirements.txt

# Run the built-in demo
python main.py --demo

# Run the real-time taxi ETL simulation
python main.py --taxi-demo --taxi-records 30

# Run executable failure/healing scenario coverage
python main.py --failure-scenarios

# Include intentional unrecoverable source/load failures
python main.py --failure-scenarios --include-hard-failures

# Run agentic autonomous mode
python main.py --taxi-demo --taxi-records 30 --autonomous-mode
```

## Documentation

Detailed design and operations documentation is available in [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md).

Autonomous mode documentation is available in [docs/AGENTIC_SELF_HEALING.md](docs/AGENTIC_SELF_HEALING.md).

## Demo

The demo simulates three consecutive pipeline runs against the same source:

| Run | Data | Drift | Outcome |
|-----|------|-------|---------|
| 1 | Baseline 100 rows | None | 100 rows loaded; schema v1 registered |
| 2 | `amount` type changed to string, `region` added, `status` removed | 3 simultaneous drift types | Coercion loss exceeds threshold → 100 rows quarantined + ERROR alert |
| 3 | Post-drift clean data | `region` added, `status` removed (no type issue) | Auto-healed: backfill + schema evolution to v2; 50 rows loaded |

## Real-Time Taxi ETL

The project now includes a real-time NYC Taxi-style ETL implementation that reuses the same self-healing framework:

```
taxi_etl/
├── producer.py             # Streams synthetic or TLC CSV records into incoming/
├── transform.py            # Cleans, validates, and enriches taxi trips
├── warehouse.py            # Raw, fact, dimension, and summary table helpers
├── realtime.py             # Polling file watcher + self-healing ETL integration
└── dashboard/app.py        # Streamlit live analytics dashboard
```

Run a complete local simulation:

```bash
python main.py --taxi-demo --taxi-records 30 --taxi-batch-size 5
```

Stream from a downloaded NYC TLC CSV:

```bash
python main.py --taxi-demo \
  --taxi-source-csv data/taxi/raw/yellow_tripdata_sample.csv \
  --taxi-records 100
```

Run the incoming-folder watcher continuously:

```bash
python main.py --taxi-stream
```

Launch the dashboard:

```bash
streamlit run taxi_etl/dashboard/app.py
```

By default, the taxi implementation writes to `data/taxi/taxi_warehouse.db`. Pass `--warehouse-db postgresql://user:pass@host/dbname` to use PostgreSQL. The warehouse contains `raw_taxi_trip`, `fact_taxi_trip`, `dim_date`, `dim_payment`, and `trip_summary`.

The taxi producer intentionally introduces a mid-stream drift in demo mode (`fare_amount` as a currency string plus a new `congestion_surcharge` column), so the existing drift detection, healing, quarantine, MTTD, and MTTR metrics are visible in a realistic ETL workflow.

## CLI Usage

```bash
# Run a CSV file through the pipeline
python main.py --source orders.csv --dest output.csv --source-name orders

# Use strict mode — quarantine any drift instead of healing
python main.py --source orders.csv --dest output.csv --strict

# With Slack alerts
python main.py --source orders.csv --dest output.csv \
  --slack-webhook https://hooks.slack.com/services/...

# JSONL source to database
python main.py --source events.jsonl --source-type jsonl \
  --dest-type db --dest sqlite:///warehouse.db \
  --source-name events
```

### All CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | — | Source file path |
| `--source-type` | `csv` | `csv` or `jsonl` |
| `--dest` | — | Destination path or DB URL |
| `--dest-type` | `csv` | `csv`, `jsonl`, `db`, `memory` |
| `--source-name` | `source` | Logical name used in schema registry |
| `--pipeline-name` | `etl_pipeline` | Name shown in alerts and run tracking |
| `--strict` | off | Quarantine on any drift; disable healing |
| `--no-coercion` | off | Disable type coercion |
| `--no-backfill` | off | Disable missing column backfill |
| `--no-evolution` | off | Disable schema auto-evolution |
| `--slack-webhook` | — | Slack incoming webhook URL |
| `--demo` | — | Run the built-in 3-run demo |
| `--failure-scenarios` | off | Run executable ETL failure and auto-healing scenario coverage |
| `--autonomous-mode` | off | Enable event-driven agentic RCA, planning, healing audit, validation, and recovery |
| `--human-approval` | off | In autonomous mode, persist the healing plan for approval instead of executing it |
| `--include-hard-failures` | off | Include intentional missing-source and invalid-destination failures in scenario coverage |
| `--taxi-demo` | off | Run the real-time taxi ETL simulation once |
| `--taxi-stream` | off | Continuously watch `data/taxi/incoming` for taxi micro-batches |
| `--taxi-records` | `30` | Number of synthetic/source records to emit for `--taxi-demo` |
| `--taxi-batch-size` | `5` | Records per incoming micro-batch |
| `--taxi-source-csv` | — | Optional NYC TLC CSV file to stream instead of synthetic records |
| `--warehouse-db` | `data/taxi/taxi_warehouse.db` | SQLAlchemy warehouse URL for taxi ETL |
| `--taxi-root` | `data/taxi` | Runtime folder for taxi incoming, processed, and SQLite state files |
| `--taxi-no-drift` | off | Disable intentional taxi drift injection |
| `--taxi-drift-after` | auto | Record number after which intentional taxi drift starts |
| `--taxi-random-failures` | off | Inject random taxi schema/type/data-quality failures for autonomous discovery |

## Programmatic Usage

```python
from config import ETLConfig, HealingConfig, AlertConfig
from pipeline.orchestrator import etl_flow

cfg = ETLConfig(
    pipeline_name="orders_pipeline",
    healing=HealingConfig(
        enable_type_coercion=True,
        enable_schema_evolution=True,
        max_coercion_loss_pct=5.0,   # quarantine batch if >5% rows fail coercion
    ),
    alerts=AlertConfig(slack_webhook_url="https://hooks.slack.com/..."),
)

summary = etl_flow(
    source_name="orders",
    source_type="csv",
    destination_type="db",
    config=cfg,
    source_path="orders.csv",
    destination_path="postgresql://user:pass@host/db",
    destination_table="orders_clean",
)
print(summary)
# {'run_id': 'run-20260503T...', 'status': 'SUCCESS', 'rows_extracted': 5000,
#  'rows_loaded': 4987, 'rows_quarantined': 13, 'drift_detected': True, ...}
```

### Custom transform function

Pass any `DataFrame -> DataFrame` function as `custom_transform` to apply business logic between extraction and schema validation:

```python
def normalize_orders(df):
    df["amount"] = df["amount"].abs()
    df["customer_id"] = df["customer_id"].str.strip()
    return df

etl_flow(..., custom_transform=normalize_orders)
```

## Healing Strategies

The `HealingEngine` applies strategies in order when drift is detected:

1. **Backfill removed columns** — inserts a null column of the expected type so downstream consumers don't break
2. **Handle added columns** — retains them (schema evolution) or drops them (strict mode)
3. **Type coercion** — attempts to cast each drifted column toward the registered type using pandas; rows that cannot be cast are individually quarantined

If the coercion loss rate exceeds `max_coercion_loss_pct`, the entire batch is quarantined rather than allowing a partially-poisoned load.

## Alert Structure

Every alert emitted includes:

```
[ERROR] Schema Drift Detected
Pipeline : orders_pipeline
Source   : orders
Run ID   : run-20260503T025058-7b98d6

Summary
-------
Schema drift detected in source 'orders'. Could NOT auto-heal.
Details: added=['region']; removed=['status']; type_changes={'amount': 'float->string'}

Root-cause hints
----------------
  • New columns detected (region). Likely an upstream producer schema migration.
    Check source changelog or producer deployment history.
  • Expected columns missing (status). Source may have dropped/renamed columns.
    Verify source DDL and any recent ALTER TABLE statements.
  • Column 'amount' changed type 'float' -> 'string'. Common causes: upstream
    cast change, CSV text promotion, or ORM mapping update.

Suggested actions
-----------------
  • Review drift_events table for full change details.
  • Notify the upstream data producer of the schema change.
  • Manual intervention required — check quarantine_records.
```

## Configuration Reference

```python
ETLConfig(
    pipeline_name="etl_pipeline",
    batch_size=1_000,
    max_retries=3,
    schema_registry=SchemaRegistryConfig(
        db_url="sqlite:///schema_registry.db",
        strict_mode=False,          # True = quarantine all drift without healing
    ),
    quarantine=QuarantineConfig(
        db_url="sqlite:///quarantine.db",
        auto_purge_days=30,
    ),
    healing=HealingConfig(
        enable_type_coercion=True,
        enable_column_backfill=True,
        enable_schema_evolution=True,
        max_coercion_loss_pct=5.0,
    ),
    alerts=AlertConfig(
        slack_webhook_url=None,     # set to enable Slack
        min_severity="WARNING",     # DEBUG | INFO | WARNING | ERROR | CRITICAL
    ),
)
```

## Storage

All state is stored in SQLite by default (zero external dependencies). Switch to any SQLAlchemy-compatible database by changing the `db_url` values:

```python
SchemaRegistryConfig(db_url="postgresql://user:pass@host/db")
QuarantineConfig(db_url="postgresql://user:pass@host/db")
```

Four tables are created automatically:

| Table | Purpose |
|-------|---------|
| `schema_versions` | Versioned schema snapshots per source |
| `quarantine_records` | Bad records with error type + root-cause hint |
| `drift_events` | Log of every drift event and healing action taken |
| `pipeline_runs` | Run-level metrics (extracted / loaded / quarantined) |

## Requirements

- Python 3.10+
- prefect >= 2.14
- sqlalchemy >= 2.0
- pydantic >= 2.0
- pandas >= 2.0
- rich >= 13.0
- httpx >= 0.25 (Slack alerts)
