# JSON schema drift and quarantine policy

This project has one canonical schema-drift implementation and one canonical row-quality quarantine implementation.

## Canonical schema drift flow

```text
loaders/discover_json_fields.py
  -> audit.json_field_discovery
  -> audit.expected_json_fields
  -> audit.v_json_schema_check
  -> quarantine.json_schema_drift_events
  -> dbt on-run-start: ensure_json_drift_policy_views()
```

`discover_json_fields.py` owns `audit.v_json_schema_check`. Legacy SQL must not recreate that view with another schema.

## Canonical expected schema

`audit.expected_json_fields` uses:

```text
dataset_name
source_file
json_path
expected_type
is_required
```

`audit.v_json_schema_check` exposes source JSON types as `actual_types`.

Old SQL expecting `value_type`, `source_system`, or singular `actual_type` for schema drift is legacy and must not be used.

## Drift statuses

```text
ok
type_mismatch
missing_in_actual
extra_in_actual
```

Schema drift detection is soft: it logs drift and Airflow continues.

## type_mismatch

`type_mismatch` means a JSON field exists, but its JSON type differs from the expected type.

RAW stores the payload unchanged. The drift check logs the issue. Type normalization happens later in `staging_cleaned` via safe casting where applicable.

Examples:

```text
"12345" -> 12345 -> row passes
"abc"   -> NULL  -> row is blocked only if the field is required
```

Safe-cast infrastructure:

```text
dbt/wb_dbt/macros/safe_cast.sql
dbt/wb_dbt/macros/ensure_safe_cast_functions.sql
audit.try_cast_date(text)
audit.try_cast_timestamp(text)
audit.try_cast_timestamptz(text)
audit.try_cast_jsonb(text)
```

`safe_cast` belongs at the typing boundary, normally `staging_cleaned`, not core/marts.

## missing_in_actual

`missing_in_actual` means a field exists in expected schema but is absent from the current JSON payload.

Optional missing fields may pass as NULL/default. Required missing fields become row-level quarantine issues if they remain NULL after normalization.

Useful views:

```sql
select * from audit.v_json_missing_fields_current;
select * from audit.v_json_missing_required_fields_current;
select * from audit.v_json_missing_optional_fields_current;
```

## extra_in_actual

`extra_in_actual` means the payload contains a field absent from expected schema.

Extra fields do not block the pipeline and do not quarantine rows. RAW keeps the full payload; typed layers ignore the field until it is accepted and modeled.

Useful view:

```sql
select * from audit.v_json_extra_fields_pending;
```

Accepting an extra field means adding it to `audit.expected_json_fields` as optional. It does not automatically add it to staging/core/marts/client exports.

## Recreated policy objects

The dbt `on-run-start` hook calls `ensure_json_drift_policy_views()` and recreates:

```text
audit.json_extra_field_decisions
audit.v_json_extra_fields_pending
audit.v_json_missing_fields_current
audit.v_json_missing_required_fields_current
audit.v_json_missing_optional_fields_current
```

## Canonical row quarantine

Canonical row-level quarantine lives in:

```text
dbt/wb_dbt/models/quarantine/row_quality
```

Manual SQL under `sql/quarantine` must not create parallel row-quality pipelines.

The old orders SQL is archived only as legacy documentation:

```text
docs/legacy/sql_quarantine/01_quarantine_orders_rows.legacy.sql
```

## Cleaned required NULL decision layer

The generic decision model is intentionally named narrowly:

```text
rq_cleaned_required_null_decisions
```

The orders-specific view is:

```text
rq_orders_cleaned_required_null_decisions
```

These models classify only `*_required_null_issues` as:

```text
bad
partial
warning
```

and expose:

```text
can_load_to_core
can_count_revenue
can_use_order_date
```

## Current architecture

```text
RAW / landing
  -> discover_json_fields.py
  -> audit.v_json_schema_check
  -> quarantine.json_schema_drift_events
  -> dbt staging
  -> dbt staging_cleaned / safe_cast / normalization
  -> dbt row_quality quarantine
  -> core
  -> marts
  -> client_exports
```
