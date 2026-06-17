# Self-Healing ETL Framework

A Prefect-based ETL framework that detects schema drift, quarantines unsafe records, applies bounded healing, emits traceable observability events, and can optionally run an agentic RCA/planning/healing loop. The project also includes a real-time taxi ETL demo and an optional Kubernetes AI-SRE extension for local Docker Desktop Kubernetes demonstrations.

## Features

- Schema drift detection across added columns, removed columns, and type changes.
- Auto-healing for missing-column backfill, type coercion, destination column evolution, and safe replay-style actions.
- Quarantine storage for bad records with error detail, root-cause hints, and manual resolution timestamps.
- Structured alerts to console and Slack.
- Versioned schema registry with automatic schema evolution when enabled.
- Prefect orchestration for extract, transform, and load stages.
- Agentic autonomous mode with event bus, observer, RCA, planner, bounded healer, validator, incident memory, and human approval queue.
- Real-time NYC Taxi-style producer, watcher, warehouse, and Streamlit dashboard.
- Optional Kubernetes AI-SRE layer that observes cluster failures, performs RCA/planning, and executes bounded K8s actions.

## Project Structure

```text
self-healing-etl/
|-- agents/                    # Observer, RCA, planner, healer, approvals, AI-SRE loop
|-- alerts/                    # Console and Slack alerting
|-- deployment/                # Dockerfiles and Kubernetes manifests
|-- docs/                      # HLD, LLD, data model, operations, K8s AI-SRE docs
|-- healing/                   # ETL healing and Kubernetes healing engines
|-- knowledge/                 # Incident memory, runbooks, healing history
|-- migrations/                # Additive DB migrations
|-- observability/             # Event bus, metrics, telemetry, traces
|-- pipeline/                  # Extract, transform, load, orchestration
|-- quarantine/                # Quarantine records and drift events
|-- scenarios/                 # ETL and K8s failure injectors/scenarios
|-- schema/                    # Schema registry and drift detector
|-- taxi_etl/                  # Taxi producer, watcher, transform, warehouse, dashboard
|-- tests/                     # Unit tests
|-- deploy_local_demo.ps1      # Local Docker Kubernetes deployment helper
|-- demo_k8s.py                # Interactive K8s self-healing demo
|-- demo.py                    # Built-in ETL lifecycle demo
|-- main.py                    # CLI entry point
`-- requirements.txt
```

## Quick Start

```bash
pip install -r requirements.txt

python main.py --demo
python main.py --failure-scenarios
python main.py --taxi-demo --taxi-records 30
python main.py --taxi-demo --taxi-records 30 --autonomous-mode
streamlit run taxi_etl/dashboard/app.py
```

Use `python -B` on Windows if cache file permissions are noisy.

## Documentation

Start with [docs/DOCUMENTATION_INDEX.md](docs/DOCUMENTATION_INDEX.md).

Key documents:

- [docs/HLD.md](docs/HLD.md): architecture and runtime modes.
- [docs/LLD.md](docs/LLD.md): component-level design.
- [docs/DATA_MODEL.md](docs/DATA_MODEL.md): tables, warehouse model, and metrics.
- [docs/AGENTIC_SELF_HEALING.md](docs/AGENTIC_SELF_HEALING.md): autonomous ETL healing loop.
- [docs/K8S_AI_SRE.md](docs/K8S_AI_SRE.md): Kubernetes AI-SRE extension and local Docker demo.
- [docs/OPERATIONS.md](docs/OPERATIONS.md): operational commands and troubleshooting.

## Built-In ETL Demo

```bash
python main.py --demo
```

The demo runs three batches against one source:

| Run | Input | Outcome |
|---|---|---|
| 1 | Baseline rows | Schema v1 registered and rows loaded. |
| 2 | Type drift, added column, removed column | Unsafe coercion loss triggers quarantine and alerting. |
| 3 | Clean post-drift rows | Missing column is backfilled, new column is retained, and schema evolves. |

## Real-Time Taxi ETL

Run a complete local simulation:

```bash
python main.py --taxi-demo --taxi-records 30 --taxi-batch-size 5
```

Run the incoming-folder watcher continuously:

```bash
python main.py --taxi-stream
```

Launch the dashboard:

```bash
streamlit run taxi_etl/dashboard/app.py
```

By default, taxi runtime files live under `data/taxi/`, including `taxi_warehouse.db`, `taxi_schema_registry.db`, and `taxi_quarantine.db`. Pass `--warehouse-db <sqlalchemy-url>` to use another warehouse database.

The taxi producer can intentionally introduce drift (`fare_amount` as currency text plus `congestion_surcharge`) so drift detection, healing, quarantine, MTTD, MTTR, and agentic events are visible in a realistic workflow.

## Autonomous Mode

```bash
python main.py --taxi-demo --taxi-records 30 --autonomous-mode
python main.py --taxi-demo --taxi-records 30 --autonomous-mode --human-approval
```

Autonomous mode persists all decisions into:

- `pipeline_events`
- `incident_history`
- `healing_actions`
- `human_approvals`

Ollama is optional. When it is unavailable or disabled, deterministic runbook rules are used.

## Kubernetes AI-SRE Local Demo

The Kubernetes extension is opt-in. It does not change existing ETL commands.

Prerequisites:

- Docker Desktop running.
- Docker Desktop Kubernetes, Minikube, or Kind reachable through `kubectl`.
- Local Python dependencies installed with `pip install -r requirements.txt`.

Deploy to local Kubernetes:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_local_demo.ps1
```

Run the live image-pull failure demo:

```bash
python demo_k8s.py --scenario image_pull
```

What the demo does:

1. Builds `self-healing-etl:latest` and `self-healing-etl-ai-sre:latest`.
2. Deploys Postgres, the ETL watcher, and the AI-SRE loop to the `self-healing-etl` namespace.
3. Patches the ETL deployment to an invalid image.
4. The AI-SRE loop detects `K8sDeploymentDegraded` and `K8sImagePullFailed`.
5. RCA/planning maps the failure to inspection, registry validation, and rollback.
6. `K8sHealingEngine` restores the previous image and deployment args.
7. The demo prints approximate MTTR.

Validated local result from Docker Desktop Kubernetes:

```text
Deployment recovered. Approximate MTTR: 17.7s
```

Supported scenarios:

```bash
python demo_k8s.py --scenario image_pull
python demo_k8s.py --scenario crash_loop
python demo_k8s.py --scenario oom
python demo_k8s.py --scenario scale_zero
python demo_k8s.py --scenario api_rate_limit
python demo_k8s.py --scenario staging_data_quality
python demo_k8s.py --scenario timeout
python demo_k8s.py --scenario concurrent_modification
```

`demo.py` remains the built-in three-run ETL lifecycle demo. The live K8s and modern ETL failure demo entry point is `demo_k8s.py`.

Modern ETL scenarios:

| Scenario | Pattern | Recovery Demonstrated |
|---|---|---|
| `api_rate_limit` | Salesforce/SaaS-style HTTP 429 | Exponential backoff plus extraction retry. |
| `staging_data_quality` | dbt/Snowflake-style not-null and unique test failures | Bad rows quarantined; clean rows continue. |
| `timeout` | Fabric/lakehouse-style connectivity timeout | Idempotent staging retry and load retry. |
| `concurrent_modification` | Databricks Delta-style concurrent write conflict | Transaction rollback request plus staging retry. |

## CLI Usage

```bash
python main.py --source orders.csv --dest output.csv --source-name orders
python main.py --source orders.csv --dest output.csv --strict
python main.py --source events.jsonl --source-type jsonl --dest-type db --dest sqlite:///warehouse.db --source-name events
python main.py --failure-scenarios --include-hard-failures
```

Important flags:

| Flag | Description |
|---|---|
| `--demo` | Run the built-in lifecycle demo. |
| `--failure-scenarios` | Run ETL failure and auto-healing scenario coverage. |
| `--include-hard-failures` | Include intentional missing-source and invalid-destination failures. |
| `--taxi-demo` | Run the taxi ETL simulation once. |
| `--taxi-stream` | Continuously watch taxi incoming files. |
| `--taxi-records` | Number of records to emit in taxi demo mode. |
| `--taxi-batch-size` | Records per taxi micro-batch. |
| `--taxi-source-csv` | Optional TLC-style CSV input. |
| `--warehouse-db` | SQLAlchemy warehouse URL. |
| `--taxi-root` | Runtime folder for taxi state. |
| `--taxi-no-drift` | Disable intentional taxi drift. |
| `--taxi-random-failures` | Inject random taxi failure patterns. |
| `--autonomous-mode` | Enable event-driven RCA, planning, healing, validation, and audit. |
| `--human-approval` | Persist plan for approval instead of executing autonomous actions. |

## Programmatic Usage

```python
from config import ETLConfig, HealingConfig
from pipeline.orchestrator import etl_flow

cfg = ETLConfig(
    pipeline_name="orders_pipeline",
    healing=HealingConfig(
        enable_type_coercion=True,
        enable_schema_evolution=True,
        max_coercion_loss_pct=5.0,
    ),
)

summary = etl_flow(
    source_name="orders",
    source_type="csv",
    destination_type="db",
    config=cfg,
    source_path="orders.csv",
    destination_path="sqlite:///warehouse.db",
    destination_table="orders_clean",
)
print(summary)
```

## Storage

SQLite is the default for local development. SQLAlchemy-compatible URLs are supported for registry, quarantine, observability, and warehouse storage. The Kubernetes demo uses PostgreSQL in-cluster through `psycopg`.

Core metadata tables:

| Table | Purpose |
|---|---|
| `schema_versions` | Versioned source schemas. |
| `quarantine_records` | Bad records and resolution status. |
| `drift_events` | Drift detection and healing summaries. |
| `pipeline_runs` | Run-level ETL metrics. |
| `pipeline_events` | Traceable ETL, agent, and K8s events. |
| `incident_history` | Historical RCA/action memory, with `source_domain` of `ETL` or `K8S`. |
| `healing_actions` | Audited autonomous actions. |
| `human_approvals` | Pending approval plans. |

## Requirements

- Python 3.10+
- Docker Desktop and `kubectl` for the optional K8s demo
- Prefect
- SQLAlchemy
- pandas
- pydantic
- rich
- Streamlit
- Kubernetes Python client
- psycopg for PostgreSQL-backed K8s demo storage
