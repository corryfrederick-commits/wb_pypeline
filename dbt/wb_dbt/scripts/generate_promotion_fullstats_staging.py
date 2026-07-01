import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal

import psycopg2


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
ENV_PATH = Path("/opt/wb_pipeline/.env")

SQL_PATH = PROJECT / "models" / "staging" / "promotion" / "stg_promotion_fullstats.sql"
YML_PATH = PROJECT / "models" / "staging" / "promotion" / "stg_promotion_fullstats.yml"
PROFILE_PATH = PROJECT / "metadata" / "promotion_fullstats_nested_profile.json"

SPEC_CSV = PROJECT / "metadata" / "auto_staging_dataset_spec.csv"
SPEC_JSON = PROJECT / "metadata" / "auto_staging_dataset_spec.json"

DATASET_NAME = "promotion_fullstats"

TECHNICAL_COLUMNS = {
    "raw_payload_id",
    "record_index",
    "root_index",
    "day_index",
    "app_index",
    "nm_index",
    "source_system",
    "dataset_name",
    "source_file",
    "source_url",
    "file_hash",
    "loaded_at",
    "raw_root",
    "raw_day",
    "raw_app",
    "raw_nm",
    "raw_record",
}

RESERVED = {
    "order", "group", "user", "select", "from", "where", "limit", "offset",
    "table", "view", "schema", "primary", "foreign", "references", "date",
}


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def normalize_payload(payload):
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def to_jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def json_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def sample_value(value):
    try:
        s = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        s = str(value)
    if len(s) > 120:
        s = s[:117] + "..."
    return s


def path_to_text(path):
    return ".".join(path)


def flatten_record(record, prefix=(), max_depth=4, skip_keys=None):
    skip_keys = skip_keys or set()
    result = {}

    if not isinstance(record, dict):
        return result

    for k, v in record.items():
        if k in skip_keys:
            continue

        path = prefix + (k,)

        if isinstance(v, dict) and max_depth > 0:
            nested = flatten_record(v, path, max_depth - 1, skip_keys=set())
            if nested:
                result.update(nested)
            else:
                result[path_to_text(path)] = v
        else:
            result[path_to_text(path)] = v

    return result


def can_int(values):
    for v in values:
        if isinstance(v, bool):
            return False
        if isinstance(v, int):
            continue
        if isinstance(v, str) and re.fullmatch(r"-?\d+", v.strip()):
            continue
        return False
    return bool(values)


def can_numeric(values):
    for v in values:
        if isinstance(v, bool):
            return False
        if isinstance(v, (int, float)):
            continue
        if isinstance(v, str) and re.fullmatch(r"-?\d+([.,]\d+)?", v.strip()):
            continue
        return False
    return bool(values)


def can_bool(values):
    allowed = {"true", "false", "t", "f", "1", "0", "yes", "no", "y", "n"}
    for v in values:
        if isinstance(v, bool):
            continue
        if isinstance(v, str) and v.strip().lower() in allowed:
            continue
        return False
    return bool(values)


def looks_like_timestamptz(values):
    iso = re.compile(
        r"^\d{4}-\d{2}-\d{2}"
        r"([T\s]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
    )
    checked = 0
    for v in values:
        if not isinstance(v, str):
            return False
        if not iso.match(v.strip()):
            return False
        checked += 1
    return checked > 0


def suggest_sql_type(values, types):
    non_null_types = {t for t in types if t != "null"}

    if not non_null_types:
        return "text"

    if non_null_types & {"object", "array"}:
        return "jsonb"

    if can_bool(values):
        return "boolean"

    if can_int(values):
        return "bigint"

    if can_numeric(values):
        return "numeric"

    if looks_like_timestamptz(values):
        return "timestamptz"

    return "text"


def to_snake(name):
    name = name.replace(".", "_")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name or "field"


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
    return "{" + ",".join(field_path.split(".")) + "}"


def sql_expr(source_json_col: str, field_path: str, sql_type: str) -> str:
    path = pg_json_path(field_path)

    if sql_type == "jsonb":
        return f"{source_json_col} #> '{path}'"

    if sql_type == "bigint":
        return f"staging.try_bigint({source_json_col} #>> '{path}')"

    if sql_type == "numeric":
        return f"staging.try_numeric({source_json_col} #>> '{path}')"

    if sql_type == "boolean":
        return f"staging.try_bool({source_json_col} #>> '{path}')"

    if sql_type == "timestamptz":
        return f"staging.try_timestamptz({source_json_col} #>> '{path}')"

    return f"nullif({source_json_col} #>> '{path}', '')"


def quote_yaml(s: str) -> str:
    return '"' + (s or "").replace('"', '\\"') + '"'


def add_stat(stats, field_path, value):
    stat = stats.setdefault(field_path, {
        "types": set(),
        "examples": [],
        "values_for_type": [],
        "non_null_count": 0,
    })

    stat["types"].add(json_type(value))

    if value is not None:
        stat["non_null_count"] += 1

        if len(stat["examples"]) < 5:
            sv = sample_value(value)
            if sv not in stat["examples"]:
                stat["examples"].append(sv)

        if len(stat["values_for_type"]) < 100:
            stat["values_for_type"].append(value)


def update_spec(expected_records: int, field_count: int):
    if SPEC_JSON.exists():
        spec = json.loads(SPEC_JSON.read_text(encoding="utf-8"))
    else:
        spec = []

    found = False
    for row in spec:
        if row.get("dataset_name") == DATASET_NAME:
            row["domain"] = "promotion"
            row["model_name"] = "stg_promotion_fullstats"
            row["main_record_path"] = "$[].days[].apps[].nms[]"
            row["record_count"] = expected_records
            row["field_count"] = field_count
            row["enabled"] = True
            row["reason"] = "manual_nested_staging_root_days_apps_nms"
            found = True

    if not found:
        spec.append({
            "dataset_name": DATASET_NAME,
            "domain": "promotion",
            "model_name": "stg_promotion_fullstats",
            "main_record_path": "$[].days[].apps[].nms[]",
            "record_count": expected_records,
            "field_count": field_count,
            "enabled": True,
            "reason": "manual_nested_staging_root_days_apps_nms",
        })

    SPEC_JSON.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

    with SPEC_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "dataset_name", "domain", "model_name", "main_record_path",
            "record_count", "field_count", "enabled", "reason",
        ])
        writer.writeheader()
        writer.writerows(spec)


def main():
    env = load_env(ENV_PATH)

    conn = psycopg2.connect(
        host=env.get("DB_HOST", "localhost"),
        port=env.get("DB_PORT", "5432"),
        dbname=env.get("DB_NAME", "wb_pipeline"),
        user=env.get("DB_USER", "wb_user"),
        password=env["DB_PASSWORD"],
    )

    sql = """
        select distinct on (source_system, dataset_name, source_file)
            id,
            source_system,
            dataset_name,
            source_file,
            source_url,
            file_hash,
            loaded_at,
            payload
        from quarantine.v_raw_payloads_schema_passed
        where dataset_name = %s
        order by
            source_system,
            dataset_name,
            source_file,
            loaded_at desc,
            id desc;
    """

    payload_rows = []

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (DATASET_NAME,))
            for row in cur.fetchall():
                payload_rows.append({
                    "id": row[0],
                    "source_system": row[1],
                    "dataset_name": row[2],
                    "source_file": row[3],
                    "source_url": row[4],
                    "file_hash": row[5],
                    "loaded_at": to_jsonable(row[6]),
                    "payload": normalize_payload(row[7]),
                })

    conn.close()

    if not payload_rows:
        raise RuntimeError("No promotion_fullstats payload found")

    root_stats = {}
    day_stats = {}
    app_stats = {}
    nm_stats = {}

    root_total = 0
    days_total = 0
    apps_total = 0
    nm_total = 0

    for row in payload_rows:
        payload = row["payload"]

        if isinstance(payload, list):
            roots = payload
        elif isinstance(payload, dict):
            roots = [payload]
        else:
            roots = []

        for root_obj in roots:
            if not isinstance(root_obj, dict):
                continue

            root_total += 1

            root_flat = flatten_record(root_obj, skip_keys={"days"})
            for field_path, value in root_flat.items():
                add_stat(root_stats, field_path, value)

            days = root_obj.get("days", [])
            if isinstance(days, dict):
                days = [days]
            if not isinstance(days, list):
                days = []

            for day_obj in days:
                if not isinstance(day_obj, dict):
                    continue

                days_total += 1

                day_flat = flatten_record(day_obj, skip_keys={"apps"})
                for field_path, value in day_flat.items():
                    add_stat(day_stats, field_path, value)

                apps = day_obj.get("apps", [])
                if isinstance(apps, dict):
                    apps = [apps]
                if not isinstance(apps, list):
                    apps = []

                for app_obj in apps:
                    if not isinstance(app_obj, dict):
                        continue

                    apps_total += 1

                    app_flat = flatten_record(app_obj, skip_keys={"nms"})
                    for field_path, value in app_flat.items():
                        add_stat(app_stats, field_path, value)

                    nms = app_obj.get("nms", [])
                    if isinstance(nms, dict):
                        nms = [nms]
                    if not isinstance(nms, list):
                        nms = []

                    for nm_obj in nms:
                        if not isinstance(nm_obj, dict):
                            continue

                        nm_total += 1

                        nm_flat = flatten_record(nm_obj)
                        for field_path, value in nm_flat.items():
                            add_stat(nm_stats, field_path, value)

    used_aliases = set(TECHNICAL_COLUMNS)

    yaml_cols = [
        ("raw_payload_id", "Технический lineage. ID строки из landing.raw_payloads."),
        ("record_index", "Порядковый номер nm-записи внутри raw payload."),
        ("root_index", "Порядковый номер элемента внутри корневого массива promotion_fullstats[]."),
        ("day_index", "Порядковый номер элемента внутри массива days[]."),
        ("app_index", "Порядковый номер элемента внутри массива apps[]."),
        ("nm_index", "Порядковый номер элемента внутри массива nms[]."),
        ("source_system", "Система-источник данных."),
        ("dataset_name", "Имя исходного dataset/endpoint mock API."),
        ("source_file", "Имя исходного JSON-файла."),
        ("source_url", "URL исходного JSON-файла."),
        ("file_hash", "Хэш исходного файла."),
        ("loaded_at", "Время загрузки raw payload в PostgreSQL."),
        ("raw_root", "Исходный JSON-объект корневого элемента promotion_fullstats[]."),
        ("raw_day", "Исходный JSON-объект day из массива days[]."),
        ("raw_app", "Исходный JSON-объект app из массива apps[]."),
        ("raw_nm", "Исходный JSON-объект nm из массива nms[]."),
        ("raw_record", "То же, что raw_nm. Унифицированное поле исходной записи для staging."),
    ]

    select_lines = []

    profile = {
        "dataset_name": DATASET_NAME,
        "main_record_path": "$[].days[].apps[].nms[]",
        "source_files": len(payload_rows),
        "root_total": root_total,
        "days_total": days_total,
        "apps_total": apps_total,
        "record_count": nm_total,
        "root_fields": [],
        "day_fields": [],
        "app_fields": [],
        "nm_fields": [],
    }

    def add_fields(group_name, stats, source_col, prefix):
        for field_path, stat in sorted(stats.items()):
            values = stat["values_for_type"]
            types = sorted(stat["types"])
            suggested_type = suggest_sql_type(values, types)
            alias = safe_alias(prefix + to_snake(field_path), used_aliases)

            select_lines.append(
                f"        {sql_expr(source_col, field_path, suggested_type)} as {alias},"
            )

            profile[f"{group_name}_fields"].append({
                "field_path": field_path,
                "column_name": alias,
                "json_types": types,
                "suggested_sql_type": suggested_type,
                "non_null_count": stat["non_null_count"],
                "examples": stat["examples"],
            })

            yaml_cols.append((
                alias,
                f"Автоматически распарсенное поле `{field_path}` из уровня `{group_name}` "
                f"dataset `{DATASET_NAME}`. Предложенный SQL-тип: {suggested_type}."
            ))

    add_fields("root", root_stats, "raw_root", "root_")
    add_fields("day", day_stats, "raw_day", "day_")
    add_fields("app", app_stats, "raw_app", "app_")
    add_fields("nm", nm_stats, "raw_nm", "nm_")

    if select_lines and select_lines[-1].endswith(","):
        select_lines[-1] = select_lines[-1].rstrip(",")

    model_sql = f"""{{{{ config(materialized='table', schema='staging', tags=['nested_staging', 'auto_staging']) }}}}

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
    where dataset_name = '{DATASET_NAME}'
    order by
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded_root as (

    select
        p.raw_payload_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        r.ordinality::integer as root_index,
        r.raw_root
    from latest_raw p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload) = 'array' then p.payload
            when jsonb_typeof(p.payload) = 'object' then jsonb_build_array(p.payload)
            else '[]'::jsonb
        end
    ) with ordinality as r(raw_root, ordinality)

),

expanded_days as (

    select
        r.raw_payload_id,
        r.source_system,
        r.dataset_name,
        r.source_file,
        r.source_url,
        r.file_hash,
        r.loaded_at,
        r.root_index,
        d.ordinality::integer as day_index,
        r.raw_root,
        d.raw_day
    from expanded_root r
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(r.raw_root -> 'days') = 'array' then r.raw_root -> 'days'
            when jsonb_typeof(r.raw_root -> 'days') = 'object' then jsonb_build_array(r.raw_root -> 'days')
            else '[]'::jsonb
        end
    ) with ordinality as d(raw_day, ordinality)

),

expanded_apps as (

    select
        d.raw_payload_id,
        d.source_system,
        d.dataset_name,
        d.source_file,
        d.source_url,
        d.file_hash,
        d.loaded_at,
        d.root_index,
        d.day_index,
        a.ordinality::integer as app_index,
        d.raw_root,
        d.raw_day,
        a.raw_app
    from expanded_days d
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(d.raw_day -> 'apps') = 'array' then d.raw_day -> 'apps'
            when jsonb_typeof(d.raw_day -> 'apps') = 'object' then jsonb_build_array(d.raw_day -> 'apps')
            else '[]'::jsonb
        end
    ) with ordinality as a(raw_app, ordinality)

),

expanded_nms as (

    select
        a.raw_payload_id,
        a.source_system,
        a.dataset_name,
        a.source_file,
        a.source_url,
        a.file_hash,
        a.loaded_at,
        a.root_index,
        a.day_index,
        a.app_index,
        n.ordinality::integer as nm_index,
        a.raw_root,
        a.raw_day,
        a.raw_app,
        n.raw_nm
    from expanded_apps a
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(a.raw_app -> 'nms') = 'array' then a.raw_app -> 'nms'
            when jsonb_typeof(a.raw_app -> 'nms') = 'object' then jsonb_build_array(a.raw_app -> 'nms')
            else '[]'::jsonb
        end
    ) with ordinality as n(raw_nm, ordinality)

),

typed as (

    select
        raw_payload_id,
        (
            row_number() over (
                partition by raw_payload_id
                order by root_index, day_index, app_index, nm_index
            )
        )::integer as record_index,
        root_index,
        day_index,
        app_index,
        nm_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_root,
        raw_day,
        raw_app,
        raw_nm,
        raw_nm as raw_record{"," if select_lines else ""}
{chr(10).join(select_lines)}
    from expanded_nms

)

select *
from typed
"""

    yml_lines = [
        "version: 2",
        "",
        "models:",
        "  - name: stg_promotion_fullstats",
        f"    description: {quote_yaml('Nested staging-модель для dataset promotion_fullstats. Одна строка = одна nm-запись внутри структуры promotion_fullstats[].days[].apps[].nms[].')}",
        "    columns:",
    ]

    not_null_cols = {
        "raw_payload_id",
        "record_index",
        "root_index",
        "day_index",
        "app_index",
        "nm_index",
        "dataset_name",
        "raw_root",
        "raw_day",
        "raw_app",
        "raw_nm",
        "raw_record",
    }

    for col, desc in yaml_cols:
        yml_lines.append(f"      - name: {col}")
        yml_lines.append(f"        description: {quote_yaml(desc)}")
        if col in not_null_cols:
            yml_lines.append("        data_tests:")
            yml_lines.append("          - not_null")

    SQL_PATH.parent.mkdir(parents=True, exist_ok=True)
    YML_PATH.parent.mkdir(parents=True, exist_ok=True)

    SQL_PATH.write_text(model_sql, encoding="utf-8")
    YML_PATH.write_text("\n".join(yml_lines) + "\n", encoding="utf-8")
    PROFILE_PATH.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")

    field_count = len(root_stats) + len(day_stats) + len(app_stats) + len(nm_stats)
    update_spec(nm_total, field_count)

    print("OK: regenerated promotion_fullstats staging")
    print("dataset:", DATASET_NAME)
    print("source_files:", len(payload_rows))
    print("root_total:", root_total)
    print("days_total:", days_total)
    print("apps_total:", apps_total)
    print("expected_nm_records:", nm_total)
    print("root_fields:", len(root_stats))
    print("day_fields:", len(day_stats))
    print("app_fields:", len(app_stats))
    print("nm_fields:", len(nm_stats))
    print()
    print("files:")
    print(SQL_PATH)
    print(YML_PATH)
    print(PROFILE_PATH)


if __name__ == "__main__":
    main()
