
BEGIN;

CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS quarantine;

DROP VIEW IF EXISTS quarantine.v_raw_payloads_schema_passed CASCADE;
DROP VIEW IF EXISTS quarantine.v_schema_warnings CASCADE;
DROP VIEW IF EXISTS quarantine.v_schema_blocked_datasets CASCADE;

DROP TABLE IF EXISTS quarantine.raw_payloads_schema_failed CASCADE;
DROP TABLE IF EXISTS quarantine.json_schema_errors CASCADE;

DROP VIEW IF EXISTS audit.v_json_schema_check CASCADE;

CREATE OR REPLACE VIEW audit.v_json_schema_check AS
WITH expected AS (
    SELECT
        source_system,
        dataset_name,
        json_path,
        value_type AS expected_type
    FROM audit.expected_json_fields
),
actual AS (
    SELECT
        source_system,
        dataset_name,
        raw_payload_id,
        source_file,
        json_path,
        value_type AS actual_type
    FROM audit.json_field_discovery
),
joined AS (
    SELECT
        COALESCE(e.source_system, a.source_system) AS source_system,
        COALESCE(e.dataset_name, a.dataset_name) AS dataset_name,
        a.raw_payload_id,
        a.source_file,
        COALESCE(e.json_path, a.json_path) AS json_path,
        e.expected_type,
        a.actual_type,
        CASE
            WHEN e.json_path IS NULL THEN 'extra_in_actual'
            WHEN a.json_path IS NULL THEN 'missing_in_actual'
            WHEN e.expected_type IS DISTINCT FROM a.actual_type THEN 'type_mismatch'
            ELSE 'ok'
        END AS check_status
    FROM expected e
    FULL OUTER JOIN actual a
      ON e.source_system = a.source_system
     AND e.dataset_name = a.dataset_name
     AND e.json_path = a.json_path
)
SELECT *
FROM joined;

CREATE TABLE quarantine.json_schema_errors (
    id BIGSERIAL PRIMARY KEY,
    raw_payload_id BIGINT,
    source_system TEXT,
    dataset_name TEXT,
    source_file TEXT,
    json_path TEXT,
    expected_type TEXT,
    actual_type TEXT,
    check_status TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'warning')),
    reason TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO quarantine.json_schema_errors (
    raw_payload_id,
    source_system,
    dataset_name,
    source_file,
    json_path,
    expected_type,
    actual_type,
    check_status,
    severity,
    reason,
    details
)
SELECT
    raw_payload_id,
    source_system,
    dataset_name,
    source_file,
    json_path,
    expected_type,
    actual_type,
    check_status,
    CASE
        WHEN check_status IN ('missing_in_actual', 'type_mismatch')
         AND json_path IN ('orders', 'orders[]')
        THEN 'critical'
        ELSE 'warning'
    END AS severity,
    CASE
        WHEN check_status = 'extra_in_actual'
            THEN 'Поле появилось в JSON, но его нет в expected-схеме'
        WHEN check_status = 'missing_in_actual'
            THEN 'Поле есть в expected-схеме, но отсутствует в текущем JSON'
        WHEN check_status = 'type_mismatch'
            THEN 'Тип поля в JSON отличается от expected-схемы'
        ELSE 'unknown schema issue'
    END AS reason,
    jsonb_build_object(
        'json_path', json_path,
        'expected_type', expected_type,
        'actual_type', actual_type,
        'check_status', check_status
    ) AS details
FROM audit.v_json_schema_check
WHERE check_status <> 'ok';

CREATE TABLE quarantine.raw_payloads_schema_failed AS
SELECT
    rp.id AS raw_payload_id,
    rp.source_system,
    rp.dataset_name,
    rp.source_file,
    rp.source_url,
    rp.file_hash,
    rp.loaded_at,
    rp.payload_type,
    rp.top_level_count,
    rp.payload,
    COUNT(e.*) FILTER (WHERE e.severity = 'critical') AS critical_errors_count,
    COUNT(e.*) FILTER (WHERE e.severity = 'warning') AS warning_errors_count,
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'json_path', e.json_path,
                'check_status', e.check_status,
                'expected_type', e.expected_type,
                'actual_type', e.actual_type,
                'severity', e.severity,
                'reason', e.reason
            )
            ORDER BY e.severity, e.json_path
        ) FILTER (WHERE e.id IS NOT NULL),
        '[]'::jsonb
    ) AS errors_json
FROM landing.raw_payloads rp
JOIN quarantine.json_schema_errors e
  ON e.raw_payload_id = rp.id
WHERE e.severity = 'critical'
GROUP BY
    rp.id,
    rp.source_system,
    rp.dataset_name,
    rp.source_file,
    rp.source_url,
    rp.file_hash,
    rp.loaded_at,
    rp.payload_type,
    rp.top_level_count,
    rp.payload;

CREATE OR REPLACE VIEW quarantine.v_schema_blocked_datasets AS
SELECT
    dataset_name,
    source_file,
    raw_payload_id,
    critical_errors_count,
    warning_errors_count,
    errors_json
FROM quarantine.raw_payloads_schema_failed
ORDER BY dataset_name, source_file, raw_payload_id;

CREATE OR REPLACE VIEW quarantine.v_schema_warnings AS
SELECT *
FROM quarantine.json_schema_errors
WHERE severity = 'warning'
ORDER BY dataset_name, source_file, json_path;

CREATE OR REPLACE VIEW quarantine.v_raw_payloads_schema_passed AS
SELECT rp.*
FROM landing.raw_payloads rp
LEFT JOIN quarantine.raw_payloads_schema_failed f
  ON f.raw_payload_id = rp.id
WHERE f.raw_payload_id IS NULL;

COMMIT;
