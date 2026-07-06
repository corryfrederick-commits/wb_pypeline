# Missing JSON fields policy

`missing_in_actual` means a field exists in `audit.expected_json_fields`, but the current source JSON payload does not contain it.

## Rule

Missing fields do not stop raw loading and do not fail Airflow by themselves.

```text
RAW loads the payload
detect_json_schema_drift logs missing_in_actual
staging extracts what exists
staging_cleaned normalizes missing values to NULL or defaults
row-level quarantine blocks rows only if required fields remain NULL
```

## Required missing fields

If a missing field is required, then rows depending on that field should not pass to core/marts after normalization.

Current required missing fields are visible in:

```sql
select *
from audit.v_json_missing_required_fields_current;
```

The concrete bad rows are handled by row-level quarantine through public staging_cleaned wrappers. If a required field is NULL after normalization, that row is excluded from public staging_cleaned and appears in `rq_*_required_null_issues`.

## Optional missing fields

If a missing field is optional, the row may pass further with NULL or a documented default.

Current optional missing fields are visible in:

```sql
select *
from audit.v_json_missing_optional_fields_current;
```

## Do not block whole datasets on missing fields

`missing_in_actual` is a schema-level signal. It should not block the whole dataset by itself.

Blocking happens only at row level when required fields are NULL after normalization.
