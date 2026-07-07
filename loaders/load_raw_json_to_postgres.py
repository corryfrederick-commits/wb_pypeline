import json
import os
import hashlib
from pathlib import Path

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

from json_sources import JSON_FILES


PROJECT_DIR = Path(os.getenv("WB_PIPELINE_PROJECT_DIR", "/opt/wb_pipeline"))
ENV_PATH = PROJECT_DIR / ".env"

load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

WB_MOCK_BASE_URL = os.getenv("WB_MOCK_BASE_URL")

# Internal SaaS identifiers.
# WB API does not return these fields; our loader assigns them.
WB_CLIENT_ID = os.getenv("WB_CLIENT_ID", "demo_client")
WB_ACCOUNT_ID = os.getenv("WB_ACCOUNT_ID", "demo_wb_account")
AUDIT_RUN_ID = os.getenv("AUDIT_RUN_ID")

DOWNLOAD_DIR = PROJECT_DIR / "data" / "tmp_downloads"


def get_dataset_name(filename: str) -> str:
    return filename.replace(".json", "")


def calculate_file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def get_payload_info(data):
    if isinstance(data, dict):
        array_lengths = [
            len(value)
            for value in data.values()
            if isinstance(value, list)
        ]

        if array_lengths:
            return "object_with_array", sum(array_lengths)

        return "object", len(data)

    if isinstance(data, list):
        return "array", len(data)

    return type(data).__name__, 1



def record_raw_payload_load_event(
    cur,
    *,
    raw_payload_id,
    client_id,
    wb_account_id,
    source_system,
    dataset_name,
    source_file,
    source_url,
    file_hash,
    payload_type,
    top_level_count,
    status,
    error_message=None,
):
    if not AUDIT_RUN_ID:
        return

    cur.execute(
        """
        INSERT INTO audit.raw_payload_load_events (
            run_id,
            raw_payload_id,
            client_id,
            wb_account_id,
            source_system,
            dataset_name,
            source_file,
            source_url,
            file_hash,
            payload_type,
            top_level_count,
            status,
            error_message
        )
        VALUES (
            %s::bigint,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (
            run_id,
            client_id,
            wb_account_id,
            source_system,
            source_file,
            file_hash
        )
        DO UPDATE SET
            raw_payload_id = EXCLUDED.raw_payload_id,
            dataset_name = EXCLUDED.dataset_name,
            source_url = EXCLUDED.source_url,
            payload_type = EXCLUDED.payload_type,
            top_level_count = EXCLUDED.top_level_count,
            status = EXCLUDED.status,
            event_at = now(),
            error_message = EXCLUDED.error_message;
        """,
        (
            int(AUDIT_RUN_ID),
            raw_payload_id,
            client_id,
            wb_account_id,
            source_system,
            dataset_name,
            source_file,
            source_url,
            file_hash,
            payload_type,
            top_level_count,
            status,
            error_message,
        ),
    )


def load_json_file(cur, json_path: Path):
    filename = json_path.name

    with open(json_path, "r", encoding="utf-8") as file:
        payload = json.load(file)

    dataset_name = get_dataset_name(filename)
    file_hash = calculate_file_hash(json_path)
    payload_type, top_level_count = get_payload_info(payload)

    source_url = None
    if WB_MOCK_BASE_URL:
        source_url = f"{WB_MOCK_BASE_URL.rstrip('/')}/{filename}"

    print("\\n[+] Обрабатываю JSON:")
    print(f"    Файл: {json_path}")
    print(f"    Dataset: {dataset_name}")
    print(f"    Тип JSON: {payload_type}")
    print(f"    Количество элементов: {top_level_count}")
    print(f"    Hash: {file_hash}")

    cur.execute(
        """
        SELECT id
        FROM landing.raw_payloads
        WHERE client_id = %s
          AND wb_account_id = %s
          AND source_system = %s
          AND source_file = %s
          AND file_hash = %s
        ORDER BY id
        LIMIT 1;
        """,
        (WB_CLIENT_ID, WB_ACCOUNT_ID, "wb_mock", filename, file_hash),
    )

    existing_row = cur.fetchone()

    if existing_row:
        existing_id = existing_row[0]
        print("[=] Такой JSON уже есть, повторно не загружаю.")
        print(f"    landing.raw_payloads.id = {existing_id}")

        record_raw_payload_load_event(
            cur,
            raw_payload_id=existing_id,
            client_id=WB_CLIENT_ID,
            wb_account_id=WB_ACCOUNT_ID,
            source_system="wb_mock",
            dataset_name=dataset_name,
            source_file=filename,
            source_url=source_url,
            file_hash=file_hash,
            payload_type=payload_type,
            top_level_count=top_level_count,
            status="skipped_duplicate",
        )

        return {
            "filename": filename,
            "status": "skipped_duplicate",
            "raw_payload_id": existing_id,
        }

    cur.execute(
        """
        INSERT INTO landing.raw_payloads (
            client_id,
            wb_account_id,
            source_system,
            dataset_name,
            source_file,
            source_url,
            file_hash,
            payload_type,
            top_level_count,
            payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            WB_CLIENT_ID,
            WB_ACCOUNT_ID,
            "wb_mock",
            dataset_name,
            filename,
            source_url,
            file_hash,
            payload_type,
            top_level_count,
            Json(payload),
        ),
    )

    raw_id = cur.fetchone()[0]

    record_raw_payload_load_event(
        cur,
        raw_payload_id=raw_id,
        client_id=WB_CLIENT_ID,
        wb_account_id=WB_ACCOUNT_ID,
        source_system="wb_mock",
        dataset_name=dataset_name,
        source_file=filename,
        source_url=source_url,
        file_hash=file_hash,
        payload_type=payload_type,
        top_level_count=top_level_count,
        status="inserted",
    )

    print("[+] JSON записан в PostgreSQL")
    print(f"    landing.raw_payloads.id = {raw_id}")

    return {
        "filename": filename,
        "status": "inserted",
        "raw_payload_id": raw_id,
    }


def main():
    if not DOWNLOAD_DIR.exists():
        raise FileNotFoundError(f"Папка со скачанными JSON не найдена: {DOWNLOAD_DIR}")

    json_paths = []

    for filename in JSON_FILES:
        path = DOWNLOAD_DIR / filename

        if path.exists():
            json_paths.append(path)
        else:
            print(f"[!] Файл не найден, пропускаю: {path}")

    if not json_paths:
        raise FileNotFoundError(f"В папке {DOWNLOAD_DIR} нет ожидаемых JSON.")

    print("[+] Начинаю загрузку JSON в PostgreSQL")
    print(f"    DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    print(f"    Найдено файлов: {len(json_paths)}")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    results = []

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS landing;")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS landing.raw_payloads (
                        id BIGSERIAL PRIMARY KEY,
                        client_id TEXT NOT NULL DEFAULT 'demo_client',
                        wb_account_id TEXT NOT NULL DEFAULT 'demo_wb_account',
                        source_system TEXT NOT NULL,
                        dataset_name TEXT NOT NULL,
                        source_file TEXT NOT NULL,
                        source_url TEXT,
                        file_hash TEXT NOT NULL,
                        loaded_at TIMESTAMPTZ DEFAULT NOW(),
                        payload_type TEXT,
                        top_level_count INTEGER,
                        payload JSONB NOT NULL
                    );
                    """
                )

                for json_path in json_paths:
                    results.append(load_json_file(cur, json_path))

        inserted_count = sum(1 for item in results if item["status"] == "inserted")
        skipped_count = sum(1 for item in results if item["status"] == "skipped_duplicate")

        print("\\n========== ИТОГ ЗАГРУЗКИ В POSTGRESQL ==========")
        print(f"[+] Новых JSON загружено: {inserted_count}")
        print(f"[=] Дубликатов пропущено: {skipped_count}")

        for item in results:
            print(
                f"    {item['filename']} | {item['status']} | raw_payload_id={item['raw_payload_id']}"
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
