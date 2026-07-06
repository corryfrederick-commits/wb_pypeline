
## Safe cast for type mismatch

Для обработки `type_mismatch` добавлена инфраструктура безопасного приведения типов.

Реализованы dbt macros:

```text
dbt/wb_dbt/macros/safe_cast.sql
dbt/wb_dbt/macros/ensure_safe_cast_functions.sql
Добавлены PostgreSQL-функции:

audit.try_cast_date(text)
audit.try_cast_timestamp(text)
audit.try_cast_timestamptz(text)
audit.try_cast_jsonb(text)

Принцип работы:

если значение можно безопасно привести к нужному типу → оно приводится
если значение нельзя безопасно привести → возвращается NULL
dbt build не падает из-за невалидного значения

Важно:

safe_cast применяется только в местах первичного приведения JSON/staging-значений к бизнес-типам
технические приведения вроде ordinality::integer или '[]'::jsonb не меняются
агрегационные cast в marts не относятся к schema drift type_mismatch

Текущая проверка показала, что в staging/staging_cleaned нет опасных бизнесовых cast, которые нужно заменить немедленно.
Инфраструктура safe cast готова для будущих изменений схемы и новых generated staging_cleaned моделей.

## Safe cast for type mismatch

Для обработки `type_mismatch` добавлена инфраструктура безопасного приведения типов.

Реализованы dbt macros:

```text
dbt/wb_dbt/macros/safe_cast.sql
dbt/wb_dbt/macros/ensure_safe_cast_functions.sql
> Добавлены PostgreSQL-функции:

audit.try_cast_date(text)
audit.try_cast_timestamp(text)
audit.try_cast_timestamptz(text)
audit.try_cast_jsonb(text)

Принцип работы:

если значение можно безопасно привести к нужному типу → оно приводится
если значение нельзя безопасно привести → возвращается NULL
dbt build не падает из-за невалидного значения

Важно:

safe_cast применяется только в местах первичного приведения JSON/staging-значений к бизнес-типам
технические приведения вроде ordinality::integer или '[]'::jsonb не меняются
агрегационные cast в marts не относятся к schema drift type_mismatch

Текущая проверка показала, что в staging/staging_cleaned нет опасных бизнесовых cast, которые нужно заменить немедленно.
Инфраструктура safe cast готова для будущих изменений схемы и новых generated staging_cleaned моделей.

## Safe cast for type mismatch

Для обработки `type_mismatch` добавлена инфраструктура безопасного приведения типов.

Реализованы dbt macros:

    dbt/wb_dbt/macros/safe_cast.sql
    dbt/wb_dbt/macros/ensure_safe_cast_functions.sql

Добавлены PostgreSQL-функции:

    audit.try_cast_date(text)
    audit.try_cast_timestamp(text)
    audit.try_cast_timestamptz(text)
    audit.try_cast_jsonb(text)

Принцип работы:

    если значение можно безопасно привести к нужному типу — оно приводится
    если значение нельзя безопасно привести — возвращается NULL
    dbt build не падает из-за невалидного значения

Важно:

    safe_cast применяется только в местах первичного приведения JSON/staging-значений к бизнес-типам
    технические приведения вроде ordinality::integer или '[]'::jsonb не меняются
    агрегационные cast в marts не относятся к schema drift type_mismatch

Текущая проверка показала, что в staging/staging_cleaned нет опасных бизнесовых cast, которые нужно заменить немедленно.

Инфраструктура safe cast готова для будущих изменений схемы и новых generated staging_cleaned моделей.

## Extra JSON fields policy

`extra_in_actual` is handled as a schema acceptance workflow.

Extra fields are stored in RAW, logged by schema drift detection, and do not block rows.

Pending extra fields are visible in:

```sql
select *
from audit.v_json_extra_fields_pending;
```

If an extra field is accepted, it is inserted into `audit.expected_json_fields` as optional and stops being reported as unknown.

## Missing JSON fields policy

`missing_in_actual` is handled as schema drift plus row-level validation.

Missing fields do not fail Airflow by themselves. Required missing fields become row-level quarantine issues if they remain NULL after staging_cleaned normalization. Optional missing fields can pass as NULL or documented defaults.

Useful views:

```sql
select * from audit.v_json_missing_fields_current;
select * from audit.v_json_missing_required_fields_current;
select * from audit.v_json_missing_optional_fields_current;
```

## Recreating extra/missing policy objects

The dbt `on-run-start` hook calls `ensure_json_drift_policy_views()`.

It recreates:

```text
audit.json_extra_field_decisions
audit.v_json_extra_fields_pending
audit.v_json_missing_fields_current
audit.v_json_missing_required_fields_current
audit.v_json_missing_optional_fields_current
```

This makes extra/missing drift policy objects reproducible after database recreation.
