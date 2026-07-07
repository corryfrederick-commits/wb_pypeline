# Audit service layer

SQL-friendly operational audit layer for the WB pipeline.

## Tables

```text
audit.load_runs
audit.dataset_runs
audit.raw_payload_load_events
audit.table_freshness
```

## Views

```sql
select * from audit.v_latest_loads;
select * from audit.v_quarantine_summary;
select * from audit.v_schema_drift_summary;
select * from audit.v_table_freshness;
select * from audit.v_pipeline_failures;
```

## Meaning

`audit.load_runs` stores one row per pipeline run.

`audit.raw_payload_load_events` stores one row per raw payload load attempt inside a concrete pipeline run.

It records:

```text
inserted
skipped_duplicate
failed
```

This keeps `landing.raw_payloads` as a deduplicated payload store, while preserving run-level loader history in audit tables.

`audit.dataset_runs` stores per-dataset run stats aggregated from `audit.raw_payload_load_events`.

Important fields:

```text
raw_payloads_loaded              inserted payloads in this run
skipped_duplicate_payloads       payloads skipped because the same source_file + hash already existed
duplicate_payloads_in_raw        duplicates among inserted raw payloads
raw_records_loaded               top-level record count from inserted payloads
```

`audit.table_freshness` stores current table freshness, row counts and max data dates.

The views provide simple SQL entry points for latest loads, quarantine problems, schema drift and failures.

## Run-level load accounting

The raw loader does not attach `run_id` to `landing.raw_payloads`.

Instead, Airflow passes `AUDIT_RUN_ID` to `loaders/load_raw_json_to_postgres.py`.

The loader writes every load attempt to:

```text
audit.raw_payload_load_events
```

Then `scripts/audit_airflow_run.py collect` aggregates `audit.dataset_runs` strictly by:

```sql
where audit.raw_payload_load_events.run_id = audit.load_runs.run_id
```

This avoids time-window ambiguity and correctly records repeated runs where all files are skipped as duplicates.

## Retry semantics

Raw payload load events use status precedence inside the same audit run:

```text
inserted > skipped_duplicate > failed
```

This prevents a successful `inserted` event from being overwritten by `skipped_duplicate` during an Airflow retry of the same run.

File-level loader errors are recorded as `failed` events in `audit.raw_payload_load_events`.
