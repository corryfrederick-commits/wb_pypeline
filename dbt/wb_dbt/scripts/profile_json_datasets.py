import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from decimal import Decimal
from datetime import date, datetime

import psycopg2


PROJECT_DIR = Path("/opt/wb_pipeline/dbt/wb_dbt")
ENV_PATH = Path("/opt/wb_pipeline/.env")

OUT_SUMMARY_JSON = PROJECT_DIR / "metadata" / "auto_dataset_summary.json"
OUT_SUMMARY_CSV = PROJECT_DIR / "metadata" / "auto_dataset_summary.csv"
OUT_FIELDS_JSON = PROJECT_DIR / "metadata" / "auto_dataset_profile.json"
OUT_FIELDS_CSV = PROJECT_DIR / "metadata" / "auto_dataset_profile.csv"


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def to_jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_payload(payload):
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def path_to_text(path):
    if not path:
        return "$"
    return ".".join(path)


def get_by_path(obj, path):
    cur = obj
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur


def find_array_candidates(obj, prefix=(), max_depth=4):
    candidates = []

    if isinstance(obj, list):
        object_count = sum(1 for x in obj if isinstance(x, dict))
        candidates.append({
            "path": path_to_text(prefix),
            "path_tuple": prefix,
            "array_length": len(obj),
            "object_count": object_count,
            "score": object_count * 1000 + len(obj),
        })

        if max_depth <= 0:
            return candidates

        for item in obj[:5]:
            if isinstance(item, dict):
                candidates.extend(find_array_candidates(item, prefix, max_depth - 1))
        return candidates

    if isinstance(obj, dict) and max_depth > 0:
        for k, v in obj.items():
            candidates.extend(find_array_candidates(v, prefix + (k,), max_depth - 1))

    return candidates


def choose_main_record_path(payloads):
    scores = defaultdict(lambda: {"array_length": 0, "object_count": 0, "score": 0})

    for payload in payloads:
        candidates = find_array_candidates(payload)

        if not candidates and isinstance(payload, dict):
            scores["$"]["array_length"] += 1
            scores["$"]["object_count"] += 1
            scores["$"]["score"] += 1

        for c in candidates:
            p = c["path"]
            scores[p]["array_length"] += c["array_length"]
            scores[p]["object_count"] += c["object_count"]
            scores[p]["score"] += c["score"]

    if not scores:
        return "$"

    best = sorted(
        scores.items(),
        key=lambda kv: (kv[1]["object_count"], kv[1]["array_length"], kv[1]["score"]),
        reverse=True,
    )[0][0]

    return best


def extract_records(payload, main_path):
    if main_path == "$":
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return [payload]
        return []

    path = tuple(main_path.split("."))
    obj = get_by_path(payload, path)

    if isinstance(obj, list):
        return obj

    if isinstance(obj, dict):
        return [obj]

    return []


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


def flatten_record(record, prefix=(), max_depth=4):
    result = {}

    if not isinstance(record, dict):
        return result

    for k, v in record.items():
        path = prefix + (k,)

        if isinstance(v, dict) and max_depth > 0:
            nested = flatten_record(v, path, max_depth - 1)
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


def to_snake(name):
    name = name.replace(".", "_")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name or "field"


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
            loaded_at,
            payload
        from quarantine.v_raw_payloads_schema_passed
        order by
            source_system,
            dataset_name,
            source_file,
            loaded_at desc,
            id desc;
    """

    rows = []

    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for row in cur.fetchall():
                payload = normalize_payload(row[5])
                rows.append({
                    "id": row[0],
                    "source_system": row[1],
                    "dataset_name": row[2],
                    "source_file": row[3],
                    "loaded_at": to_jsonable(row[4]),
                    "payload": payload,
                })

    conn.close()

    by_dataset = defaultdict(list)
    for r in rows:
        by_dataset[r["dataset_name"]].append(r)

    summary = []
    field_rows = []

    for dataset_name, dataset_rows in sorted(by_dataset.items()):
        payloads = [r["payload"] for r in dataset_rows]
        main_path = choose_main_record_path(payloads)

        field_stats = {}
        record_count = 0

        for r in dataset_rows:
            records = extract_records(r["payload"], main_path)

            for idx, rec in enumerate(records, start=1):
                if not isinstance(rec, dict):
                    rec = {"value": rec}

                record_count += 1
                flat = flatten_record(rec)

                for field_path, value in flat.items():
                    stat = field_stats.setdefault(field_path, {
                        "non_null_count": 0,
                        "types": set(),
                        "examples": [],
                        "values_for_type": [],
                    })

                    t = json_type(value)
                    stat["types"].add(t)

                    if value is not None:
                        stat["non_null_count"] += 1

                        if len(stat["examples"]) < 5:
                            sv = sample_value(value)
                            if sv not in stat["examples"]:
                                stat["examples"].append(sv)

                        if len(stat["values_for_type"]) < 100:
                            stat["values_for_type"].append(value)

        summary.append({
            "dataset_name": dataset_name,
            "source_files": len(dataset_rows),
            "main_record_path": main_path,
            "record_count": record_count,
            "field_count": len(field_stats),
        })

        for field_path, stat in sorted(field_stats.items()):
            values = stat["values_for_type"]
            types = sorted(stat["types"])
            field_rows.append({
                "dataset_name": dataset_name,
                "main_record_path": main_path,
                "record_count": record_count,
                "field_path": field_path,
                "suggested_column_name": to_snake(field_path),
                "json_types": ",".join(types),
                "non_null_count": stat["non_null_count"],
                "null_or_missing_count": max(record_count - stat["non_null_count"], 0),
                "suggested_sql_type": suggest_sql_type(values, types),
                "examples": " | ".join(stat["examples"]),
            })

    PROJECT_DIR.joinpath("metadata").mkdir(parents=True, exist_ok=True)

    OUT_SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_FIELDS_JSON.write_text(json.dumps(field_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    with OUT_SUMMARY_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "dataset_name",
            "source_files",
            "main_record_path",
            "record_count",
            "field_count",
        ])
        writer.writeheader()
        writer.writerows(summary)

    with OUT_FIELDS_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "dataset_name",
            "main_record_path",
            "record_count",
            "field_path",
            "suggested_column_name",
            "json_types",
            "non_null_count",
            "null_or_missing_count",
            "suggested_sql_type",
            "examples",
        ])
        writer.writeheader()
        writer.writerows(field_rows)

    print("OK: dataset profiling finished")
    print("datasets:", len(summary))
    print("field rows:", len(field_rows))
    print()
    print("files:")
    print(OUT_SUMMARY_JSON)
    print(OUT_SUMMARY_CSV)
    print(OUT_FIELDS_JSON)
    print(OUT_FIELDS_CSV)
    print()
    print("summary:")
    for s in summary:
        print(
            f"{s['dataset_name']:35s} "
            f"path={s['main_record_path']:20s} "
            f"records={s['record_count']:5d} "
            f"fields={s['field_count']:4d}"
        )


if __name__ == "__main__":
    main()
