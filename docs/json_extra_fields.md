# Extra JSON fields policy

`extra_in_actual` means the source JSON contains a field that is not present in `audit.expected_json_fields`.

## Rule

Extra fields do not block the pipeline.

```text
RAW keeps the full JSON payload
detect_json_schema_drift logs extra_in_actual
staging/staging_cleaned/core/marts continue to run
```

Extra fields are not row-level quarantine issues. A new field does not make an existing row invalid.

## Pending extra fields

Pending extra fields are visible in:

```sql
select *
from audit.v_json_extra_fields_pending;
```

## Accepting an extra field

If an extra field is useful or harmless, it can be accepted into `audit.expected_json_fields` as optional.

Accepting means:

```text
the field is known
the field is optional
it should not be reported as extra_in_actual anymore
```

Accepting does not automatically add the field to staging, core, marts, or client exports. That is a separate modeling decision.

## Do not block on extra

`extra_in_actual` should not fail Airflow and should not quarantine rows. It is handled as a schema acceptance workflow.
