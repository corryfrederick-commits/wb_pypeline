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
            text = json.dumps(value, ensure_ascii=False)
        else:
            text = str(value)

        return text[:500]
    except Exception:
        return None


def walk_json(value, path=""):
    """
    Рекурсивно обходит JSON.

    Примеры путей:
    orders
    orders[]
    orders[].id
    orders[].price
    orders[].skus
    orders[].skus[]
    """

    value_type = get_value_type(value)

    if path:
        yield path, value_type, sample_to_text(value)

    if isinstance(value, dict):
        for key, child in value.items():
            child_path = key if not path else f"{path}.{key}"
            yield from walk_json(child, child_path)

    elif isinstance(value, list):
        array_item_path = f"{path}[]"

        for item in value:
            if isinstance(item, dict):
                yield array_item_path, "object", sample_to_text(item)

                for key, child in item.items():
                    child_path = f"{array_item_path}.{key}"
                    yield from walk_json(child, child_path)

            elif isinstance(item, list):
                yield array_item_path, "array", sample_to_text(item)
                yield from walk_json(item, array_item_path)

            else:
                yield array_item_path, get_value_type(item), sample_to_text(item)


def recreate_audit_tables(cur):
    cur.execute("CREATE SCHEMA IF NOT EXISTS audit;")

    cur.execute("DROP VIEW IF EXISTS audit.v_json_schema_check;")
    cur.execute("DROP TABLE IF EXISTS audit.json_field_discovery;")
    cur.execute("DROP TABLE IF EXISTS audit.expected_json_fields;")

    cur.execute(
        """
        CREATE TABLE audit.json_field_discovery (
            id BIGSERIAL PRIMARY KEY,
            source_system TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            raw_payload_id BIGINT NOT NULL,
            json_path TEXT NOT NULL,
            value_type TEXT NOT NULL,
            sample_value TEXT,
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            UNIQUE (
                source_system,
                dataset_name,
                source_file,
                json_path,
                value_type
            )
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE audit.expected_json_fields (
            id BIGSERIAL PRIMARY KEY,
            dataset_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            json_path TEXT NOT NULL,
            expected_type TEXT NOT NULL,
            is_required BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            UNIQUE (
                dataset_name,
                source_file,
                json_path
            )
        );
        """
    )

    cur.execute(
        """
        CREATE OR REPLACE VIEW audit.v_json_schema_check AS
        WITH actual_agg AS (
            SELECT
                dataset_name,
                source_file,
                json_path,
                ARRAY_AGG(DISTINCT value_type ORDER BY value_type) AS actual_types,
                MIN(sample_value) AS sample_value
            FROM audit.json_field_discovery
            GROUP BY
                dataset_name,
                source_file,
                json_path
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
        """
    )


def discover_fields(cur):
    cur.execute(
        """
        SELECT DISTINCT ON (source_system, dataset_name, source_file)
            id,
            source_system,
            dataset_name,
            source_file,
            payload
        FROM landing.raw_payloads
        ORDER BY
            source_system,
            dataset_name,
            source_file,
            loaded_at DESC,
            id DESC;
        """
    )

    rows = cur.fetchall()

    print(f"[+] Найдено latest raw payloads: {len(rows)}")

    inserted_count = 0

    for raw_payload_id, source_system, dataset_name, source_file, payload in rows:
        print(f"\n[+] Анализирую: {dataset_name} | {source_file} | raw_id={raw_payload_id}")

        local_count = 0

        for json_path, value_type, sample_value in walk_json(payload):
            cur.execute(
                """
                INSERT INTO audit.json_field_discovery (
                    source_system,
                    dataset_name,
                    source_file,
                    raw_payload_id,
                    json_path,
                    value_type,
                    sample_value
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    source_system,
                    dataset_name,
                    source_file,
                    json_path,
                    value_type
                )
                DO NOTHING;
                """,
                (
                    source_system,
                    dataset_name,
                    source_file,
                    raw_payload_id,
                    json_path,
                    value_type,
                    sample_value,
                ),
            )

            if cur.rowcount == 1:
                inserted_count += 1
                local_count += 1

        print(f"    discovered fields: {local_count}")

    print(f"\n[+] Всего новых discovered fields: {inserted_count}")


def main():
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
                recreate_audit_tables(cur)
                discover_fields(cur)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
