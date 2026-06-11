# Operations Guide

## Common Commands

```bash
python main.py --demo
python main.py --failure-scenarios
python main.py --failure-scenarios --include-hard-failures
python main.py --taxi-demo --taxi-records 30 --taxi-batch-size 5
python main.py --taxi-demo --taxi-records 30 --autonomous-mode
python main.py --taxi-demo --taxi-records 30 --autonomous-mode --human-approval
python main.py --taxi-stream
streamlit run taxi_etl/dashboard/app.py
powershell -ExecutionPolicy Bypass -File .\deploy_local_demo.ps1
python demo_k8s.py --scenario image_pull
python demo_k8s.py --scenario api_rate_limit
python demo_k8s.py --scenario staging_data_quality
```

Use `python -B` on Windows if `__pycache__` permissions are noisy:

```bash
python -B main.py --failure-scenarios
```

## Expected Healthy Outputs

For the default scenario suite:

- Process return code should be `0`.
- Scenario table should contain `SUCCESS` rows.
- There should be no default rows for missing source or invalid load destination.
- Quarantine rows are expected for partial coercion, strict mode, transform failures, resource guard failures, and unresolved join keys.

For taxi demo:

- Records are produced into `data/taxi/incoming`.
- Processed files move to `data/taxi/processed`.
- Failed input files move to `data/taxi/failed`.
- Warehouse metrics show total trips and revenue.
- MTTD is populated when drift is detected.
- MTTR is `N/A` until quarantine records are manually resolved.

## Runtime Files

| Path | Purpose |
|---|---|
| `data/taxi/incoming` | Micro-batches waiting for processing. |
| `data/taxi/processed` | Successfully processed micro-batches. |
| `data/taxi/failed` | Files that failed in the taxi watcher. |
| `data/taxi/taxi_warehouse.db` | Default SQLite taxi warehouse. |
| `data/taxi/taxi_schema_registry.db` | Taxi schema registry. |
| `data/taxi/taxi_quarantine.db` | Taxi quarantine, drift events, pipeline runs, and metrics source. |

## Autonomous Mode Checks

After running `--autonomous-mode`, verify:

```sql
select count(*) from pipeline_events;
select count(*) from healing_actions;
select event_type, count(*) from pipeline_events group by event_type;
```

If `--human-approval` is enabled, inspect:

```sql
select status, count(*) from human_approvals group by status;
```

The CLI routes Prefect runtime state to a temporary folder to avoid user-home and OneDrive permission issues.

## Dashboard Refresh And Scope

The dashboard defaults to a 120-second auto-refresh to avoid overly aggressive reloads during demos. Use the sidebar `Refresh now` button for immediate reloads.

Use `Observability scope` to choose `All pipelines` or a specific pipeline name. `All pipelines` is recommended when demonstrating Kubernetes AI-SRE because K8s traces may use a different `pipeline_name` than the taxi ETL pipeline.

If Live Events, Root Cause Analysis, or Healing Timeline show failures while Pipeline Health or Agent Decisions appear empty, check that the scope is not filtering to only the taxi pipeline.

## Kubernetes Local Demo Operations

Deploy the local Docker Kubernetes demo:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_local_demo.ps1
```

The script checks Docker and `kubectl`, builds local images, applies Kubernetes manifests, restarts ETL and AI-SRE deployments, and waits for readiness.

Run a live failure injection:

```bash
python demo_k8s.py --scenario image_pull
```

Other scenarios:

```bash
python demo_k8s.py --scenario crash_loop
python demo_k8s.py --scenario oom
python demo_k8s.py --scenario scale_zero
```

Modern ETL failure scenarios do not require Kubernetes:

```bash
python demo_k8s.py --scenario api_rate_limit
python demo_k8s.py --scenario staging_data_quality
python demo_k8s.py --scenario timeout
python demo_k8s.py --scenario concurrent_modification
```

Check cluster state:

```bash
kubectl get pods -n self-healing-etl
kubectl logs deployment/ai-sre -n self-healing-etl --tail=80
kubectl rollout status deployment/self-healing-etl -n self-healing-etl --timeout=120s
```

Expected healthy local K8s state:

- `postgres-0` is `1/1 Running`.
- `ai-sre` is `1/1 Running`.
- Current `self-healing-etl` pod is `1/1 Running`.
- Old ETL pods may briefly show `Terminating` during rollback or rollout.

## Quarantine Review

Quarantined records include:

- pipeline name
- source name
- run ID
- raw record JSON
- error type
- error detail
- root-cause hint
- schema version
- quarantine timestamp
- resolution status
- resolution timestamp

Manual resolution should call `QuarantineStore.mark_resolved(record_ids)`. This sets `resolved=True` and `resolved_at=<current UTC time>`, which feeds MTTR.

Example:

```python
from quarantine.store import QuarantineStore

store = QuarantineStore("sqlite:///data/taxi/taxi_quarantine.db")
records = store.get_quarantined(limit=10)
store.mark_resolved([record.id for record in records[:2]])
print(store.stats(pipeline_name="taxi_realtime_pipeline"))
```

## Failure Triage

| Symptom | Likely Cause | Action |
|---|---|---|
| `PermissionError` when writing output | Destination path locked, directory used as file, or restricted location. | Choose a writable file path or run hard-failure drill intentionally. |
| `FileNotFoundError` during extract | Missing source path. | Fix source path and rerun. |
| Full batch quarantined as `HEALING_FAILED` | Coercion loss exceeded threshold. | Inspect bad values, adjust upstream data, or tune loss threshold. |
| Records quarantined as `COERCION_FAILURE` | Some values could not be cast safely. | Review failed records and repair source data. |
| Records quarantined as `TRANSFORM_ERROR` | Custom transform raised an exception. | Check transform logic and input shape. |
| Drift detected but not healed | Strict mode enabled or healing disabled. | Disable strict mode or enable the relevant healing strategy. |
| Loader schema issue | Destination table missing new columns or incompatible types. | Framework adds missing columns; for stricter DBs, update warehouse DDL. |
| PowerShell blocks `deploy_local_demo.ps1` | Script execution policy. | Run `powershell -ExecutionPolicy Bypass -File .\deploy_local_demo.ps1`. |
| AI-SRE pod exits immediately | Old image, cluster access issue, or DB URL issue. | Rerun deploy script, then inspect `kubectl logs deployment/ai-sre -n self-healing-etl --tail=120`. |
| ETL pod exits with `--source is required` | Old rollback logic cleared deployment args. | Rebuild/redeploy current code; rollback now preserves previous command and args. |
| Image-pull demo appears recovered while old pod is still running | Old readiness script or incomplete rollout check. | Use current `demo_k8s.py`; it validates the latest Deployment generation. |

## Recommended Validation Checklist

1. Run `python -B main.py --failure-scenarios`.
2. Confirm the process exits with code `0`.
3. Confirm no default scenario row has status `FAILED`.
4. Run `python -B main.py --taxi-demo --taxi-records 30`.
5. Confirm taxi warehouse has records in fact and summary tables.
6. Confirm quarantine stats show MTTD after drift.
7. Mark a quarantine record resolved and confirm MTTR appears.

## Production Hardening

- Replace SQLite with PostgreSQL for registry, quarantine, and warehouse.
- Stream batches instead of materializing all batches in memory.
- Add strong warehouse DDL management for evolved schemas.
- Add Airflow or Prefect deployments for scheduled production execution.
- Add service-level monitoring for data freshness, row-count anomalies, and warehouse load failures.
- Add authentication and role-based access for quarantine review.
- Add unit and integration tests for each healing scenario.
- Replace local K8s demo secrets and `latest` image tags before production use.
- Scope Kubernetes RBAC by namespace where possible and separate inspection permissions from mutation permissions.
