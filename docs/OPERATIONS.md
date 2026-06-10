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
