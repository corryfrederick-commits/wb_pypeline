import argparse
import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


PROJECT_DIR = Path(os.getenv("WB_PIPELINE_PROJECT_DIR", "/opt/wb_pipeline"))
ENV_PATH = PROJECT_DIR / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_value_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
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


def sample_to_text(value):
    try:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)[:500]
        return str(value)[:500]
    except Exception:
        return None


def walk_json(value, path=""):
    if path:
        yield path, get_value_type(value), sample_to_text(value)

    if isinstance(value, dict):
        for key, child in value.items():
            child_path = key if not path else f"{path}.{key}"
            yield from walk_json(child, child_path)

    elif isinstance(value, list):
        item_path = f"{path}[]"
        for item in value:
            if isinstance(item, dict):
                yield item_path, "object", sample_to_text(item)
                for key, child in item.items():
                    yield from walk_json(child, f"{item_path}.{key}")
            elif isinstance(item, list):
                yield item_path, "array", sample_to_text(item)
                yield from walk_json(item, item_path)
            else:
                yield item_path, get_value_type(item), sample_to_text(item)


def ensure_objects(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS quarantine;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit.json_field_discovery (
            id BIGSERIAL PRIMARY KEY,
            source_system TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            raw_payload_id BIGINT NOT NULL,
            json_path TEXT NOT NULL,
            value_type TEXT NOT NULL,
            sample_value TEXT,
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_system, dataset_name, source_file, json_path, value_type)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit.expected_json_fields (
            id BIGSERIAL PRIMARY KEY,
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            json_path TEXT NOT NULL,
            expected_type TEXT NOT NULL,
            is_required BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (dataset_name, source_file, json_path)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS quarantine.json_schema_drift_events (
            id BIGSERIAL PRIMARY KEY,
            run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            json_path TEXT NOT NULL,
            expected_type TEXT,
            actual_types TEXT[],
            sample_value TEXT,
            check_status TEXT NOT NULL,
            severity TEXT NOT NULL,
            action_taken TEXT NOT NULL
        );
    """)

    cur.execute("DROP VIEW IF EXISTS audit.v_json_extra_fields_pending;")
    cur.execute("DROP VIEW IF EXISTS audit.v_json_missing_required_fields_current;")
    cur.execute("DROP VIEW IF EXISTS audit.v_json_missing_optional_fields_current;")
    cur.execute("DROP VIEW IF EXISTS audit.v_json_missing_fields_current;")
    cur.execute("DROP VIEW IF EXISTS audit.v_json_schema_check;")

    cur.execute("""
        CREATE OR REPLACE VIEW audit.v_json_schema_check AS
        WITH actual_agg AS (
            SELECT
                dataset_name,
                source_file,
                json_path,
                ARRAY_AGG(DISTINCT value_type ORDER BY value_type) AS actual_types,
                MIN(sample_value) AS sample_value
            FROM audit.json_field_discovery
            GROUP BY dataset_name, source_file, json_path
        ),
        expected_vs_actual AS (
            SELECT
                e.dataset_name,
                e.source_file,
                e.json_path,
                e.expected_type,
                a.actual_types,
                a.sample_value,
                CASE
                    WHEN a.json_path IS NULL THEN 'missing_in_actual'
                    WHEN NOT (e.expected_type = ANY(a.actual_types)) THEN 'type_mismatch'
                    ELSE 'ok'
                END AS check_status
            FROM audit.expected_json_fields e
            LEFT JOIN actual_agg a
              ON a.dataset_name = e.dataset_name
             AND a.source_file = e.source_file
             AND a.json_path = e.json_path
        ),
        extra_actual AS (
            SELECT
                a.dataset_name,
                a.source_file,
                a.json_path,
                NULL::TEXT AS expected_type,
                a.actual_types,
                a.sample_value,
                'extra_in_actual' AS check_status
            FROM actual_agg a
            LEFT JOIN audit.expected_json_fields e
              ON e.dataset_name = a.dataset_name
             AND e.source_file = a.source_file
             AND e.json_path = a.json_path
            WHERE e.json_path IS NULL
        )
        SELECT * FROM expected_vs_actual
        UNION ALL
        SELECT * FROM extra_actual;
    """)


def refresh_discovery(cur):
    cur.execute("TRUNCATE TABLE audit.json_field_discovery RESTART IDENTITY;")

    cur.execute("""
        SELECT DISTINCT ON (source_system, dataset_name, source_file)
            id, source_system, dataset_name, source_file, payload
        FROM landing.raw_payloads
        ORDER BY source_system, dataset_name, source_file, loaded_at DESC, id DESC;
    """)

    rows = cur.fetchall()
    print(f"[+] latest raw payloads: {len(rows)}")

    if not rows:
        raise RuntimeError("landing.raw_payloads is empty")

    total = 0

    for raw_payload_id, source_system, dataset_name, source_file, payload in rows:
        local = 0
        for json_path, value_type, sample_value in walk_json(payload):
            cur.execute("""
                INSERT INTO audit.json_field_discovery (
                    source_system, dataset_name, source_file,
                    raw_payload_id, json_path, value_type, sample_value
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    source_system, dataset_name, source_file, json_path, value_type
                )
                DO UPDATE SET
                    raw_payload_id = EXCLUDED.raw_payload_id,
                    sample_value = EXCLUDED.sample_value,
                    discovered_at = NOW();
            """, (
                source_system, dataset_name, source_file,
                raw_payload_id, json_path, value_type, sample_value
            ))
            local += 1
            total += 1

        print(f"[+] {dataset_name}: discovered fields={local}")

    print(f"[+] total discovered fields={total}")


def baseline(cur):
    cur.execute("""
        WITH ranked AS (
            SELECT
                dataset_name,
                source_file,
                json_path,
                value_type,
                ROW_NUMBER() OVER (
                    PARTITION BY dataset_name, source_file, json_path
                    ORDER BY CASE WHEN value_type = 'null' THEN 2 ELSE 1 END, value_type
                ) AS rn
            FROM audit.json_field_discovery
        )
        INSERT INTO audit.expected_json_fields (
            dataset_name, source_file, json_path, expected_type, is_required
        )
        SELECT dataset_name, source_file, json_path, value_type, TRUE
        FROM ranked
        WHERE rn = 1
        ON CONFLICT (dataset_name, source_file, json_path) DO NOTHING;
    """)

    cur.execute("SELECT COUNT(*) FROM audit.expected_json_fields;")
    print(f"[+] expected fields={cur.fetchone()[0]}")


def log_drift(cur):
    cur.execute("""
        SELECT
            check_status,
            COUNT(*)
        FROM audit.v_json_schema_check
        GROUP BY check_status
        ORDER BY check_status;
    """)

    print("\n========== schema drift summary ==========")
    for status, count in cur.fetchall():
        print(f"{status}: {count}")

    cur.execute("""
        INSERT INTO quarantine.json_schema_drift_events (
            dataset_name,
            source_file,
            json_path,
            expected_type,
            actual_types,
            sample_value,
            check_status,
            severity,
            action_taken
        )
        SELECT
            dataset_name,
            source_file,
            json_path,
            expected_type,
            actual_types,
            sample_value,
            check_status,
            CASE
                WHEN check_status = 'type_mismatch' THEN 'warning'
                WHEN check_status = 'missing_in_actual' THEN 'warning'
                WHEN check_status = 'extra_in_actual' THEN 'info'
                ELSE 'info'
            END AS severity,
            CASE
                WHEN check_status = 'type_mismatch'
                    THEN 'Logged drift. Downstream cleaned/core models should cast safely or quarantine unsafe values.'
                WHEN check_status = 'missing_in_actual'
                    THEN 'Logged drift. Downstream models should use NULL/default for missing field.'
                WHEN check_status = 'extra_in_actual'
                    THEN 'Logged drift. Extra field ignored until accepted into expected schema/mapping.'
                ELSE 'No action.'
            END AS action_taken
        FROM audit.v_json_schema_check
        WHERE check_status <> 'ok';
    """)

    cur.execute("""
        SELECT COUNT(*)
        FROM audit.v_json_schema_check
        WHERE check_status <> 'ok';
    """)
    drift_count = cur.fetchone()[0]

    print(f"\n[+] drift records logged={drift_count}")
    print("[+] soft mode: Airflow continues even if drift exists")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "check"], default="check")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                ensure_objects(cur)
                refresh_discovery(cur)

                if args.mode == "baseline":
                    baseline(cur)

                log_drift(cur)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
