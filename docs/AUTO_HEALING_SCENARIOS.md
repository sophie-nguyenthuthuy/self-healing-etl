# Auto-Healing And Failure Scenario Catalog

## Purpose

This catalog maps ETL failure modes to the current framework behavior. It is intended for demos, testing, and explaining which failures are automatically healed, quarantined, or left as manual rerun cases.

## Default Scenario Suite

Run:

```bash
python -B main.py --failure-scenarios
```

Expected result:

- Exit code `0`.
- All scenario rows show `SUCCESS`.
- Recoverable failures are healed.
- Unsafe records or batches are quarantined.
- No intentional missing-source or invalid-destination hard failures are included.

## Scenario Matrix

| Scenario | Category | Expected Framework Behavior | Evidence |
|---|---|---|---|
| Added column | Schema drift auto-healing | Retain new column and evolve schema. | `schema_evolved=True`; new schema version registered. |
| Removed column | Schema drift auto-healing | Backfill missing column with typed nulls. | Batch loads without schema evolution. |
| Numeric string type change | Type drift auto-healing | Coerce numeric strings to expected numeric type. | All rows load. |
| Partial coercion failure | Type drift partial healing | Load batch and quarantine failed rows. | `COERCION_FAILURE` records in quarantine. |
| Coercion loss threshold exceeded | Healing fallback quarantine | Quarantine full batch. | `HEALING_FAILED` records in quarantine. |
| Strict mode drift | Policy-controlled failure | Quarantine full drifted batch. | `SCHEMA_DRIFT` records in quarantine. |
| Custom transform exception | Transformation failure | Quarantine full batch. | `TRANSFORM_ERROR` records in quarantine. |
| Resource guard exception | Memory/resource failure | Quarantine full batch. | `TRANSFORM_ERROR` records with resource guard detail. |
| Join missing dimension member | Join auto-healing | Map unknown key to an unknown dimension member. | All rows load with fallback dimension member. |
| Join duplicate dimension keys | Join auto-healing | Deduplicate dimension rows before join. | All rows load with latest effective dimension row. |
| Join unresolved key | Join failure quarantine | Quarantine full batch. | `TRANSFORM_ERROR` records in quarantine. |
| Loader new column | Loading auto-healing | Add missing DB table column before append. | Destination table contains new column. |

## Hard-Failure Drill Suite

Run:

```bash
python -B main.py --failure-scenarios --include-hard-failures
```

This intentionally adds:

| Scenario | Why It Is Hard Failure |
|---|---|
| Missing source file | There is no input to extract; pipeline must fail until source path is corrected. |
| Invalid load destination | The destination is not writable as requested; pipeline must fail until destination is corrected. |

These cases are intentionally opt-in because they are useful for manual rerun documentation but should not be part of the clean auto-healing evidence run.

## Taxi Drift Scenarios

The taxi demo injects drift by default:

| Drift | Example | Expected Behavior |
|---|---|---|
| Currency-formatted amount | `fare_amount="$15.50"` | Transform normalizes values and/or healing coerces numeric type. |
| Added column | `congestion_surcharge=2.5` | Schema evolution keeps new field when enabled. |
| Data-quality issue | non-positive `trip_distance` or `fare_amount` | Transform removes invalid rows from clean output or causes quarantine depending on failure path. |

Disable drift:

```bash
python -B main.py --taxi-demo --taxi-no-drift
```

## Kubernetes AI-SRE Scenarios

The Kubernetes scenarios run against a real local cluster and are separate from `--failure-scenarios`.

Deploy the local demo first:

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_local_demo.ps1
```

Then inject a Kubernetes failure:

| Scenario | Command | Expected Behavior |
|---|---|---|
| Image pull failure | `python demo_k8s.py --scenario image_pull` | ETL deployment is patched to an invalid image; AI-SRE detects degradation/image-pull failure and rolls back to the previous image. |
| Crash loop | `python demo_k8s.py --scenario crash_loop` | ETL container exits immediately; AI-SRE detects crash-loop signals and attempts restart/rollback/log inspection. |
| OOM | `python demo_k8s.py --scenario oom` | ETL memory limit is reduced to `1Mi`; OOM remediation is approval-oriented because resource changes affect stability/cost. |
| Scale zero | `python demo_k8s.py --scenario scale_zero` | ETL deployment is scaled to zero; AI-SRE detects a degraded deployment and plans scale/restart actions. |

Validated local Docker Desktop result for `image_pull`:

```text
Deployment recovered. Approximate MTTR: 17.7s
```

## Modern ETL Staging And Connectivity Scenarios

The unified live demo entry point also covers modern ETL failures without requiring a Kubernetes cluster:

```bash
python demo_k8s.py --scenario api_rate_limit
python demo_k8s.py --scenario staging_data_quality
python demo_k8s.py --scenario timeout
python demo_k8s.py --scenario concurrent_modification
```

| Scenario | Platform Pattern | Failure Injected | Recovery Evidence |
|---|---|---|---|
| `api_rate_limit` | Salesforce and SaaS extractors | Emits an `ExtractFailed` event with HTTP `429 Too Many Requests`. | RCA classifies `API Rate Limit`; plan executes `apply_exponential_backoff` and `retry_extraction`. |
| `staging_data_quality` | dbt and Snowflake staging tests | Adds duplicate keys and unexpected null keys to a staging-style taxi batch. | Violating rows are inserted into `quarantine_records`; clean rows continue; autonomous loop records isolate/replay actions. |
| `timeout` | Microsoft Fabric/lakehouse connectivity | Emits a destination timeout while writing a staging batch. | RCA classifies `Connectivity Timeout`; plan executes `retry_staging_task` and `retry_loading`. |
| `concurrent_modification` | Databricks Delta write conflict | Emits a concurrent modification failure during staging write. | RCA classifies `Concurrent Staging Write`; plan executes `rollback_transaction` and `retry_staging_task`. |

These scenarios persist events and healing actions to a local SQLite observability DB printed by the demo command.

## Manual Rerun Cases

Some failures should remain manual because automatic repair would risk data correctness:

- Missing source files or inaccessible source systems.
- Invalid destination path or database outage.
- Excessive coercion loss.
- Unresolved join keys with no configured unknown member.
- Transform failures caused by code defects.
- Resource limits caused by oversized batches.
- Any strict-mode schema drift when policy requires review.

## Validation Queries

SQLite examples:

```sql
select pipeline_name, source_name, status, rows_loaded, rows_quarantined
from pipeline_runs
order by started_at desc;

select source_name, drift_type, healed, healing_action, detected_at
from drift_events
order by detected_at desc;

select source_name, error_type, resolved, quarantined_at, resolved_at
from quarantine_records
order by quarantined_at desc;
```
