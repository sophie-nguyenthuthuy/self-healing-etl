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

