# ETL Failure Scenarios That Require Manual Re-Run

This runbook summarizes failure scenarios where retries or self-healing are not enough and an engineer must manually re-run part or all of an ETL pipeline. It is written for this self-healing ETL project, but uses patterns from enterprise ETL platforms such as Airflow, Azure Data Factory, Databricks/Spark, AWS Glue, Snowflake, and streaming file ingestion.

## Source References

- Apache Airflow documents task retries, clearing task instances, and preserved task history when a task is retried or cleared: [Airflow DAG Runs](https://airflow.apache.org/docs/apache-airflow/3.0.0/core-concepts/dag-run.html).
- Azure Data Factory supports re-running from failed activities rather than always starting a full pipeline over: [ADF pipeline and trigger troubleshooting](https://learn.microsoft.com/en-us/azure/data-factory/pipeline-trigger-troubleshoot-guide) and [Rerun activities inside pipelines](https://azure.microsoft.com/en-us/blog/rerun-activities-inside-your-data-factory-pipelines/).
- Databricks explains that Structured Streaming checkpoints allow restart from the last successful checkpoint, while checkpoint corruption or incompatible query changes may require checkpoint recovery or reset: [Structured Streaming checkpoints](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/checkpoints) and [Recover from streaming checkpoint failure](https://learn.microsoft.com/en-us/azure/databricks/ldp/recover-streaming).
- AWS Glue observability documents categories such as out-of-memory, permission, throttling, and disk-no-space errors, and CloudWatch metrics for disk and S3 I/O: [AWS Glue observability metrics](https://docs.aws.amazon.com/glue/latest/dg/monitor-observability.html) and [CloudWatch metrics for AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/monitoring-awsglue-with-cloudwatch-metrics.html).
- AWS notes data skew can cause local disk pressure, poor scaling, low CPU use, and job failures: [Detect and handle data skew on AWS Glue](https://aws.amazon.com/blogs/big-data/detect-and-handle-data-skew-on-aws-glue/).
- Spark SQL tuning guidance covers partitioning, caching, join strategy, and skew join optimization: [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning).
- Snowflake COPY supports `VALIDATION_MODE` for pre-load error discovery and `ON_ERROR` controls whether loads continue, skip files, or abort: [Snowflake COPY INTO table](https://docs.snowflake.com/sql-reference/sql/copy-into-table).

## When Manual Re-Run Is Required

Manual re-run is required when the failed run cannot be safely resumed by automatic retry because the original failure changed external state, left partial data, consumed bad checkpoints, or depends on human correction. In this project, that usually means one of the following:

- Fix data, schema, config, credentials, or destination state.
- Decide the safe replay point: whole pipeline, failed file, failed micro-batch, failed activity, or only quarantined rows.
- Ensure the target is idempotent before re-running.
- Confirm duplicates will not be created in `raw_taxi_trip`, `fact_taxi_trip`, downstream CSV/JSONL outputs, or warehouse tables.

## Re-Run Scope Decision

| Scope | Use When | Preconditions | Example Command |
|---|---|---|---|
| Re-run failed record set | Bad rows were quarantined and upstream data is otherwise valid | Correct records or transform rules; mark old quarantine records resolved only after successful replay | Programmatic replay from `quarantine_records` |
| Re-run failed micro-batch/file | One incoming file failed due to parsing, schema, or load issue | File corrected or destination schema fixed; processed-file movement is consistent | Move file from `failed/` to `incoming/`, run `python main.py --taxi-stream --taxi-root <root>` |
| Re-run failed activity/stage | Extract succeeded but transform/load failed | Stage output is durable and idempotent | Airflow clear failed task; ADF rerun from failed activity |
| Re-run full pipeline | Source extraction, schema baseline, checkpoint, or warehouse state is uncertain | Target tables can be truncated/replaced or deduplicated | `python main.py --taxi-demo ...` or normal `python main.py --source ...` |
| Reset and replay stream | Checkpoint is corrupted or incompatible after query/source changes | Replay source is available; sink is idempotent or cleaned | Reset checkpoint, replay from source offsets/files |

## Failure Scenarios

### 1. Source Data Unavailable

**Symptoms**

- Source file missing, locked, empty, or partially written.
- API/database source returns connection errors, timeout, 404, 403, or rate limit.
- Taxi incoming folder receives `.tmp` or incomplete files.

**Why Auto-Retry May Not Be Enough**

Retries help transient source outages, but not missing files, wrong paths, expired credentials, or source-side corrections that arrive later.

**Manual Action**

1. Confirm source availability and file size/checksum.
2. If the file was partial, replace it with the complete file.
3. Move the corrected file back to `incoming/`.
4. Re-run only that file or micro-batch when possible.

**Avoid Duplicate Loads**

- Use `trip_id` or a run/file identifier as a dedup key.
- Re-run to a staging table first if destination idempotency is uncertain.

### 2. Bad Input Format or Parser Failure

**Symptoms**

- CSV delimiter/quote escape issues.
- Invalid JSONL line.
- Date strings cannot parse.
- Unexpected encoding or binary characters.

**Why Auto-Retry May Not Be Enough**

The same parser error will repeat until the file or file-format config is fixed. Snowflake recommends validating staged files with `VALIDATION_MODE` to return errors before loading, which maps to our need for a pre-load validation step.

**Manual Action**

1. Inspect the rejected file and parser error.
2. Correct file format settings or clean the source file.
3. Re-run the failed file only.
4. Keep the original bad file in an audit folder if required.

**Recommended Automation**

- Add a pre-ingestion validation task.
- Route invalid files to `failed/` with the parser error and line number.

### 3. Schema Drift That Cannot Be Safely Healed

**Symptoms**

- Required column removed.
- Type change causes high coercion loss.
- Column meaning changes while name stays the same.
- New nested/object field appears but downstream table cannot represent it.

**Why Auto-Retry May Not Be Enough**

Retries do not solve semantic changes. The self-healing framework can detect added columns, removed columns, and type changes, but manual approval may be needed when business meaning or data loss risk is unclear.

**Manual Action**

1. Review `drift_events.details_json`.
2. Review sample quarantined records.
3. Decide whether to evolve schema, add a mapping rule, or reject the feed.
4. Re-run failed batch after updating transform/schema rules.

**Project-Specific Check**

- `QuarantineStore.stats()` should show MTTD and unresolved quarantine count.
- If records were manually corrected, use `mark_resolved()` after successful replay.

### 4. Data Quality Rule Failure

**Symptoms**

- `trip_distance <= 0`
- `fare_amount <= 0`
- `pickup_datetime >= dropoff_datetime`
- Nulls in required business columns.
- Referential integrity failure against dimension tables.

**Why Auto-Retry May Not Be Enough**

Bad records will keep failing until source data or business rules are corrected. Auto-filling values can hide real source defects.

**Manual Action**

1. Quarantine invalid rows.
2. Confirm whether values are truly invalid or rules are too strict.
3. Correct source data or update quality rules.
4. Replay only quarantined rows where possible.

**Manual Re-Run Pattern**

- Re-run failed records, not the whole dataset, when records have stable identifiers.
- Recompute aggregates after replay.

### 5. Duplicate or Non-Idempotent Load

**Symptoms**

- Re-run creates duplicate rows in fact table.
- Aggregates double-count revenue/trips.
- Raw table appends the same file multiple times.

**Why Auto-Retry May Not Be Enough**

Automatic retry can be dangerous if the load committed partially before failure. A manual decision is needed to delete, merge, or deduplicate target rows before replay.

**Manual Action**

1. Identify the run ID, file name, batch ID, or business key.
2. Check what was already committed.
3. Clean or deduplicate target rows.
4. Re-run the failed unit.

**Recommended Automation**

- Add `source_file`, `run_id`, and `loaded_at` columns to warehouse tables.
- Use merge/upsert semantics for fact tables.
- Use unique constraints where supported.

### 6. Partial Load or Transaction Boundary Failure

**Symptoms**

- Raw table loaded but fact table did not.
- Fact table loaded but summary table refresh failed.
- File moved to `processed/` even though downstream analytics failed.

**Why Auto-Retry May Not Be Enough**

The pipeline state and warehouse state may disagree. A blind rerun can duplicate raw/fact rows or skip the file because it was already moved.

**Manual Action**

1. Compare file location with warehouse row counts.
2. Verify `pipeline_runs` status and row counts.
3. Decide whether to replay from raw table, original file, or clean target rows and rerun.
4. Refresh derived summary tables after fact consistency is restored.

### 7. Destination Schema or Permission Failure

**Symptoms**

- Warehouse table missing.
- Column does not exist.
- Permission denied on create/alter/insert.
- Database locked or unavailable.
- Snowflake/warehouse load aborts due to `ON_ERROR = ABORT_STATEMENT`.

**Why Auto-Retry May Not Be Enough**

Permissions and DDL failures generally persist until a user grants access or changes target schema. In this project, OneDrive-backed SQLite files have also shown locking and disk I/O errors, so `--taxi-root` should point to a local temp/runtime folder for demos.

**Manual Action**

1. Fix permissions or destination DDL.
2. Confirm whether any rows were inserted before failure.
3. Re-run from the failed file/activity.
4. If target state is unknown, run validation queries before replay.

### 8. Streaming Checkpoint Failure

**Symptoms**

- Stream cannot restart.
- Checkpoint metadata is corrupted.
- Query changes are incompatible with previous checkpoint.
- Replaying starts from the wrong offset or duplicates output.

**Why Auto-Retry May Not Be Enough**

Databricks/Spark checkpoints are designed to restart from the last committed micro-batch, but checkpoint loss/corruption or incompatible query changes can require manual checkpoint recovery/reset.

**Manual Action**

1. Stop the stream.
2. Backup the existing checkpoint.
3. Determine last committed batch/offset.
4. If resetting checkpoint, ensure source data is replayable and sink is idempotent.
5. Re-run from a known safe offset/file boundary.

**Project Mapping**

- The taxi demo uses file movement rather than Spark checkpoints.
- Equivalent state is `incoming/`, `processed/`, `failed/`, plus warehouse rows and `pipeline_runs`.

### 9. Memory, Disk, and Resource Exhaustion

**Symptoms**

- Out-of-memory.
- Executor/container killed.
- Disk no space.
- Shuffle spill is huge.
- One partition/task runs much longer than others.
- Low CPU use while job is still running.

**Why Auto-Retry May Not Be Enough**

Retrying the same workload with the same partitioning and memory settings often fails again. AWS Glue and Spark guidance both point to skew, disk pressure, partitioning, and join strategy as root causes.

**Manual Action**

1. Inspect memory, disk, shuffle, and skew metrics.
2. Identify oversized files, skewed keys, or high-cardinality joins.
3. Repartition, salt skewed keys, broadcast smaller dimensions, or increase worker resources.
4. Re-run after changing partitioning/resource config.

**Recommended Automation**

- Capture per-run row counts, file sizes, partition counts, and memory/disk metrics.
- Alert when one file or partition dominates the batch.

### 10. Orchestrator or Trigger Failure

**Symptoms**

- Pipeline did not start.
- Trigger fired twice.
- Activity was canceled.
- Parent pipeline marked success despite a middle task failure.
- Worker/agent went offline.

**Why Auto-Retry May Not Be Enough**

The work may not have run at all, may have run twice, or may have partially run. Airflow notes that DAG run status can be affected by leaf task trigger rules, and ADF supports rerunning from activity failures.

**Manual Action**

1. Confirm whether extract/load work actually happened.
2. Check task/activity-level status, not just pipeline-level status.
3. Re-run from failed task/activity if upstream outputs are valid.
4. Re-run full pipeline if task state is unreliable.

### 11. External Service Throttling or Quota Failure

**Symptoms**

- HTTP 429.
- Cloud provider throttling.
- Database max connections exceeded.
- Warehouse suspended or concurrency limit reached.

**Why Auto-Retry May Not Be Enough**

Short retries may help, but persistent quota or scheduling conflicts require changing concurrency, backoff, or capacity.

**Manual Action**

1. Verify service status and quota limits.
2. Reduce parallelism or increase backoff.
3. Resume/increase warehouse capacity if needed.
4. Re-run failed batch after capacity is restored.

### 12. Code, Dependency, or Environment Failure

**Symptoms**

- Missing package.
- Incompatible library version.
- Changed model/schema code not deployed to worker.
- Environment variable missing.

**Why Auto-Retry May Not Be Enough**

The same code/environment will fail repeatedly until deployment is fixed.

**Manual Action**

1. Pin/fix dependencies.
2. Deploy the corrected package.
3. Run a smoke test with a small input.
4. Re-run failed activities or full pipeline based on target state.

## Manual Re-Run Checklist

Use this before every manual re-run:

- Identify failed run ID, source file, batch number, and destination table.
- Classify the failure: data, ingestion, transform, load, orchestration, checkpoint, or resource.
- Confirm whether any output committed.
- Decide re-run scope.
- Make the target idempotent: delete, merge, deduplicate, or write to staging.
- Preserve the failed input and error context.
- Apply the fix.
- Re-run the smallest safe unit.
- Validate row counts, aggregates, quarantine count, and drift events.
- Mark quarantined records resolved only after successful replay.

## Project-Specific Commands

Run taxi demo with intentional drift:

```bash
python main.py --taxi-demo --taxi-records 30 --taxi-batch-size 5 --taxi-drift-after 15
```

Run taxi demo without manufactured drift:

```bash
python main.py --taxi-demo --taxi-records 30 --taxi-batch-size 5 --taxi-no-drift
```

Use a local runtime folder to avoid OneDrive SQLite locking:

```bash
python main.py --taxi-demo --taxi-root C:\Users\<user>\AppData\Local\Temp\self_healing_taxi
```

Run continuous taxi file watcher:

```bash
python main.py --taxi-stream --taxi-root C:\Users\<user>\AppData\Local\Temp\self_healing_taxi
```

Launch dashboard:

```bash
streamlit run taxi_etl/dashboard/app.py
```

## When Not To Manually Re-Run

Do not manually re-run immediately when:

- The target table already has unknown partial data and no run/file key exists.
- The stream checkpoint is suspect and the source is not replayable.
- The transform logic changed but downstream consumers have not approved the new schema.
- A data quality failure indicates a real upstream defect that must be fixed first.
- The same resource failure has repeated and no partition/resource change has been made.

## Automation Opportunities

These are good next steps for this self-healing ETL project:

- Add a `source_file` and `batch_id` lineage column to raw/fact tables.
- Add a replay command for `failed/` files and quarantined records.
- Add idempotent warehouse merge/upsert logic for `fact_taxi_trip`.
- Add data quality result tables with rule name, failed row count, and sample records.
- Add pre-load validation mode for CSV/JSONL before calling `etl_flow`.
- Add resource telemetry capture for memory, runtime, file size, and row count.
- Add a rerun decision report that recommends `record`, `file`, `stage`, or `full` replay.

Ollama or another local model can be useful for assisted diagnosis, such as classifying an error message or suggesting a likely rerun scope. It should not replace deterministic safety checks for idempotency, checkpoint state, or target table cleanup.
