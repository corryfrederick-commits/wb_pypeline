import csv
import json
import re
from collections import defaultdict
from pathlib import Path

PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
SUMMARY_PATH = PROJECT / "metadata" / "auto_dataset_summary.json"
PROFILE_PATH = PROJECT / "metadata" / "auto_dataset_profile.json"

OUT_SPEC_CSV = PROJECT / "metadata" / "auto_staging_dataset_spec.csv"
OUT_SPEC_JSON = PROJECT / "metadata" / "auto_staging_dataset_spec.json"

AUTO_BASE = PROJECT / "models" / "staging" / "auto"

# Эти orders уже покрыты нашей ручной dbt-моделью staging.stg_orders_current
COVERED_BY_ORDERS_CURRENT = {
    "orders_fbs_new",
    "orders_fbs_current",
    "orders_fbs_archive",
    "orders_dbs_new",
    "orders_dbs_completed",
    "orders_dbw_new",
    "orders_dbw_completed",
    "orders_pickup_new",
}

# Пока пропускаем nested complex dataset. Его отдельно разберём позже.
MANUAL_LATER = {
    "promotion_fullstats",
}

TECHNICAL_COLUMNS = {
    "raw_payload_id",
    "record_index",
    "source_system",
    "dataset_name",
    "source_file",
    "source_url",
    "file_hash",
    "loaded_at",
    "raw_record",
}

RESERVED = {
    "order", "group", "user", "select", "from", "where", "limit", "offset",
    "table", "view", "schema", "primary", "foreign", "references", "date",
}

def domain_from_dataset(dataset_name: str) -> str:
    if dataset_name.startswith("items_"):
        return "items"
    if dataset_name.startswith("orders_"):
        return "orders"
    if dataset_name.startswith("finance_"):
        return "finance"
    if dataset_name.startswith("report_"):
        return "reports"
    if dataset_name.startswith("tariffs_"):
        return "tariffs"
    if dataset_name.startswith("analytics_"):
        return "analytics"
    if dataset_name.startswith("promotion_"):
        return "promotion"
    if dataset_name.startswith("communications_"):
        return "communications"
    if dataset_name.startswith("fbw_"):
        return "fbw"
    if dataset_name.startswith("general_"):
        return "general"
    return dataset_name.split("_", 1)[0]

def safe_model_name(dataset_name: str) -> str:
    return "stg_" + re.sub(r"[^a-zA-Z0-9_]+", "_", dataset_name).lower()

def safe_alias(name: str, used: set) -> str:
    alias = re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_").lower()
    alias = re.sub(r"_+", "_", alias)

    if not alias:
        alias = "field"

    if alias in RESERVED:
        alias = alias + "_value"

    if alias in TECHNICAL_COLUMNS:
        alias = "src_" + alias

    base = alias
    i = 2
    while alias in used:
        alias = f"{base}_{i}"
        i += 1

    used.add(alias)
    return alias

def pg_json_path(field_path: str) -> str:
    parts = field_path.split(".")
    return "{" + ",".join(parts) + "}"

def sql_expr(field_path: str, sql_type: str) -> str:
    path = pg_json_path(field_path)

    if sql_type == "jsonb":
        return f"raw_record #> '{path}'"

    if sql_type == "bigint":
        return f"staging.try_bigint(raw_record #>> '{path}')"

    if sql_type == "numeric":
        return f"staging.try_numeric(raw_record #>> '{path}')"

    if sql_type == "boolean":
        return f"staging.try_bool(raw_record #>> '{path}')"

    if sql_type == "timestamptz":
        return f"staging.try_timestamptz(raw_record #>> '{path}')"

    return f"nullif(raw_record #>> '{path}', '')"

def records_array_sql(main_path: str) -> str:
    if main_path == "$":
        return """case
            when jsonb_typeof(p.payload) = 'array' then p.payload
            when jsonb_typeof(p.payload) = 'object' then jsonb_build_array(p.payload)
            else '[]'::jsonb
        end"""

    path = "{" + ",".join(main_path.split(".")) + "}"

    return f"""case
            when jsonb_typeof(p.payload #> '{path}') = 'array' then p.payload #> '{path}'
            when jsonb_typeof(p.payload #> '{path}') = 'object' then jsonb_build_array(p.payload #> '{path}')
            else '[]'::jsonb
        end"""

def quote_yaml(s: str) -> str:
    return '"' + (s or "").replace('"', '\\"') + '"'

summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))

profile_by_dataset = defaultdict(list)
for row in profile:
    profile_by_dataset[row["dataset_name"]].append(row)

spec = []

for s in summary:
    dataset_name = s["dataset_name"]
    record_count = int(s["record_count"])
    main_path = s["main_record_path"]

    if dataset_name in COVERED_BY_ORDERS_CURRENT:
        reason = "covered_by_stg_orders_current"
        enabled = False
    elif dataset_name in MANUAL_LATER:
        reason = "manual_later_nested_structure"
        enabled = False
    elif record_count <= 0:
        reason = "zero_records_detected"
        enabled = False
    else:
        reason = "auto_staging"
        enabled = True

    spec.append({
        "dataset_name": dataset_name,
        "domain": domain_from_dataset(dataset_name),
        "model_name": safe_model_name(dataset_name),
        "main_record_path": main_path,
        "record_count": record_count,
        "field_count": int(s["field_count"]),
        "enabled": enabled,
        "reason": reason,
    })

OUT_SPEC_JSON.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

with OUT_SPEC_CSV.open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "dataset_name", "domain", "model_name", "main_record_path",
        "record_count", "field_count", "enabled", "reason",
    ])
    writer.writeheader()
    writer.writerows(spec)

generated = []

for item in spec:
    if not item["enabled"]:
        continue

    dataset_name = item["dataset_name"]
    domain = item["domain"]
    model_name = item["model_name"]
    main_path = item["main_record_path"]

    domain_dir = AUTO_BASE / domain
    domain_dir.mkdir(parents=True, exist_ok=True)

    field_rows = profile_by_dataset[dataset_name]

    used_aliases = set(TECHNICAL_COLUMNS)

    select_lines = [
        "        raw_payload_id,",
        "        record_index,",
        "        source_system,",
        "        dataset_name,",
        "        source_file,",
        "        source_url,",
        "        file_hash,",
        "        loaded_at,",
        "        raw_record,",
    ]

    yaml_cols = [
        ("raw_payload_id", "Технический lineage. ID строки из landing.raw_payloads."),
        ("record_index", "Порядковый номер записи внутри главного массива исходного JSON. Нумерация начинается с 1."),
        ("source_system", "Система-источник данных."),
        ("dataset_name", "Имя исходного dataset/endpoint mock API."),
        ("source_file", "Имя исходного JSON-файла."),
        ("source_url", "URL исходного JSON-файла."),
        ("file_hash", "Хэш исходного файла."),
        ("loaded_at", "Время загрузки raw payload в PostgreSQL."),
        ("raw_record", "Исходный JSON-объект одной записи без потери структуры."),
    ]

    for fr in field_rows:
        field_path = fr["field_path"]
        suggested_type = fr["suggested_sql_type"]
        suggested_col = fr["suggested_column_name"]

        alias = safe_alias(suggested_col, used_aliases)
        expr = sql_expr(field_path, suggested_type)

        select_lines.append(f"        {expr} as {alias},")

        desc = (
            f"Автоматически распарсенное поле `{field_path}` из JSON dataset `{dataset_name}`. "
            f"Предложенный SQL-тип: {suggested_type}. "
            f"JSON-типы в профиле: {fr.get('json_types', '')}."
        )
        yaml_cols.append((alias, desc))

    # remove last comma
    select_lines[-1] = select_lines[-1].rstrip(",")

    records_expr = records_array_sql(main_path)

    sql = f"""{{{{ config(materialized='table', schema='staging', tags=['auto_staging']) }}}}

with latest_raw as (

    select distinct on (source_system, dataset_name, source_file)
        id as raw_payload_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload
    from {{{{ source('quarantine', 'v_raw_payloads_schema_passed') }}}}
    where dataset_name = '{dataset_name}'
    order by
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded as (

    select
        p.raw_payload_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        x.ordinality::integer as record_index,
        x.raw_record
    from latest_raw p
    cross join lateral jsonb_array_elements(
        {records_expr}
    ) with ordinality as x(raw_record, ordinality)

),

typed as (

    select
{chr(10).join(select_lines)}
    from expanded

)

select *
from typed
"""

    yml_lines = [
        "version: 2",
        "",
        "models:",
        f"  - name: {model_name}",
        f"    description: {quote_yaml(f'Автоматически сгенерированная staging-модель для dataset {dataset_name}. Главный record path: {main_path}. Одна строка = одна запись из главного массива JSON.')}",
        "    columns:",
    ]

    for col, desc in yaml_cols:
        yml_lines.append(f"      - name: {col}")
        yml_lines.append(f"        description: {quote_yaml(desc)}")
        if col in {"raw_payload_id", "record_index", "dataset_name", "raw_record"}:
            yml_lines.append("        data_tests:")
            yml_lines.append("          - not_null")

    sql_path = domain_dir / f"{model_name}.sql"
    yml_path = domain_dir / f"{model_name}.yml"

    sql_path.write_text(sql, encoding="utf-8")
    yml_path.write_text("\n".join(yml_lines) + "\n", encoding="utf-8")

    generated.append({
        "dataset_name": dataset_name,
        "model_name": model_name,
        "domain": domain,
        "main_record_path": main_path,
        "record_count": item["record_count"],
        "sql_path": str(sql_path),
        "yml_path": str(yml_path),
    })

print("OK: auto staging models generated")
print("enabled/generated:", len(generated))
print("skipped:", len(spec) - len(generated))
print()
print("generated models:")
for g in generated:
    print(
        f"{g['model_name']:35s} "
        f"dataset={g['dataset_name']:35s} "
        f"path={g['main_record_path']:25s} "
        f"records={g['record_count']}"
    )

print()
print("spec files:")
print(OUT_SPEC_JSON)
print(OUT_SPEC_CSV)
