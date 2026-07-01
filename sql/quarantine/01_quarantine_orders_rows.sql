BEGIN;

CREATE SCHEMA IF NOT EXISTS quarantine;

DROP VIEW IF EXISTS quarantine.v_orders_current_for_core;
DROP VIEW IF EXISTS quarantine.v_orders_current_bad;
DROP VIEW IF EXISTS quarantine.v_orders_current_partial;
DROP VIEW IF EXISTS quarantine.v_orders_current_good;

DROP TABLE IF EXISTS quarantine.orders_current_quality;
DROP TABLE IF EXISTS quarantine.orders_current_issues;

CREATE TABLE quarantine.orders_current_issues (
    id BIGSERIAL PRIMARY KEY,
    raw_payload_id BIGINT NOT NULL,
    record_index INTEGER NOT NULL,
    dataset_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    order_flow TEXT,
    order_kind TEXT,
    order_id BIGINT,
    issue_level TEXT NOT NULL,
    issue_code TEXT NOT NULL,
    problem_field TEXT,
    problem_value TEXT,
    details TEXT,
    raw_record JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (issue_level IN ('bad', 'partial', 'warning'))
);

INSERT INTO quarantine.orders_current_issues (
    raw_payload_id, record_index, dataset_name, source_file,
    order_flow, order_kind, order_id,
    issue_level, issue_code, problem_field, problem_value, details, raw_record
)
SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'bad', 'missing_any_order_identifier',
       'order_id/order_uid/rid/order_code',
       NULL,
       'Нет ни order_id, ни order_uid, ни rid, ни order_code. Нельзя безопасно идентифицировать заказ.',
       raw_record
FROM staging.orders_current
WHERE order_id IS NULL
  AND NULLIF(order_uid, '') IS NULL
  AND NULLIF(rid, '') IS NULL
  AND NULLIF(order_code, '') IS NULL

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'bad', 'invalid_order_flow',
       'order_flow',
       order_flow,
       'order_flow должен быть одним из: fbs, dbs, dbw, pickup.',
       raw_record
FROM staging.orders_current
WHERE order_flow IS NULL
   OR order_flow NOT IN ('fbs', 'dbs', 'edbs', 'dbspickuppoint', 'dbw', 'pickup', 'pickuppoint', 'selfpickup')

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'bad', 'invalid_order_kind',
       'order_kind',
       order_kind,
       'order_kind должен быть одним из: new, current, completed, archive.',
       raw_record
FROM staging.orders_current
WHERE order_kind IS NULL
   OR order_kind NOT IN ('new', 'current', 'completed', 'archive')

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'bad', 'raw_record_not_object',
       'raw_record',
       jsonb_typeof(raw_record),
       'raw_record должен быть JSON object.',
       raw_record
FROM staging.orders_current
WHERE jsonb_typeof(raw_record) <> 'object';

WITH row_keys AS (
    SELECT
        raw_payload_id,
        record_index,
        dataset_name,
        source_file,
        order_flow,
        order_kind,
        order_id,
        COALESCE(order_id::TEXT, NULLIF(order_uid, ''), NULLIF(rid, ''), NULLIF(order_code, '')) AS order_identity,
        raw_record
    FROM staging.orders_current
),
duplicated AS (
    SELECT dataset_name, order_identity, COUNT(*) AS rows_count
    FROM row_keys
    WHERE order_identity IS NOT NULL
    GROUP BY dataset_name, order_identity
    HAVING COUNT(*) > 1
)
INSERT INTO quarantine.orders_current_issues (
    raw_payload_id, record_index, dataset_name, source_file,
    order_flow, order_kind, order_id,
    issue_level, issue_code, problem_field, problem_value, details, raw_record
)
SELECT r.raw_payload_id, r.record_index, r.dataset_name, r.source_file,
       r.order_flow, r.order_kind, r.order_id,
       'bad', 'duplicate_order_identity_in_dataset',
       'order_identity',
       r.order_identity,
       'Один идентификатор заказа несколько раз встретился внутри одного dataset_name.',
       r.raw_record
FROM row_keys r
JOIN duplicated d
  ON d.dataset_name = r.dataset_name
 AND d.order_identity = r.order_identity;

WITH cross_flow AS (
    SELECT
        order_id,
        STRING_AGG(DISTINCT order_flow, ', ' ORDER BY order_flow) AS flows
    FROM staging.orders_current
    WHERE order_id IS NOT NULL
    GROUP BY order_id
    HAVING COUNT(DISTINCT order_flow) > 1
)
INSERT INTO quarantine.orders_current_issues (
    raw_payload_id, record_index, dataset_name, source_file,
    order_flow, order_kind, order_id,
    issue_level, issue_code, problem_field, problem_value, details, raw_record
)
SELECT s.raw_payload_id, s.record_index, s.dataset_name, s.source_file,
       s.order_flow, s.order_kind, s.order_id,
       'bad', 'order_id_in_multiple_flows',
       'order_id',
       s.order_id::TEXT,
       'Один order_id встретился в нескольких order_flow: ' || c.flows,
       s.raw_record
FROM staging.orders_current s
JOIN cross_flow c
  ON c.order_id = s.order_id;

INSERT INTO quarantine.orders_current_issues (
    raw_payload_id, record_index, dataset_name, source_file,
    order_flow, order_kind, order_id,
    issue_level, issue_code, problem_field, problem_value, details, raw_record
)
SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'partial', 'missing_order_id_using_alternate_identifier',
       'order_id',
       COALESCE(NULLIF(order_uid, ''), NULLIF(rid, ''), NULLIF(order_code, '')),
       'order_id отсутствует, но есть order_uid/rid/order_code.',
       raw_record
FROM staging.orders_current
WHERE order_id IS NULL
  AND COALESCE(NULLIF(order_uid, ''), NULLIF(rid, ''), NULLIF(order_code, '')) IS NOT NULL

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'partial', 'missing_created_at',
       'created_at',
       NULL,
       'created_at отсутствует. Заказ можно хранить, но нельзя корректно использовать в аналитике по дате заказа.',
       raw_record
FROM staging.orders_current
WHERE created_at IS NULL

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'partial', 'missing_price',
       'price',
       NULL,
       'price отсутствует. Заказ можно хранить, но нельзя использовать для расчёта выручки.',
       raw_record
FROM staging.orders_current
WHERE price IS NULL

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'partial', 'negative_price',
       'price',
       price::TEXT,
       'price отрицательная. Заказ можно хранить, но нельзя безопасно использовать для revenue-аналитики.',
       raw_record
FROM staging.orders_current
WHERE price IS NOT NULL
  AND price < 0

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'partial', 'missing_nm_id',
       'nm_id',
       NULL,
       'nm_id отсутствует. Заказ можно хранить, но товарная аналитика будет неполной.',
       raw_record
FROM staging.orders_current
WHERE nm_id IS NULL

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'partial', 'missing_article',
       'article',
       NULL,
       'article отсутствует. Заказ можно хранить, но аналитика по артикулу будет неполной.',
       raw_record
FROM staging.orders_current
WHERE article IS NULL;

INSERT INTO quarantine.orders_current_issues (
    raw_payload_id, record_index, dataset_name, source_file,
    order_flow, order_kind, order_id,
    issue_level, issue_code, problem_field, problem_value, details, raw_record
)
SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'warning', 'final_price_greater_than_price',
       'final_price',
       final_price::TEXT,
       'final_price больше price. Это не блокирует заказ.',
       raw_record
FROM staging.orders_current
WHERE price IS NOT NULL
  AND final_price IS NOT NULL
  AND final_price > price

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'warning', 'suspicious_delivery_type',
       'delivery_type',
       delivery_type,
       'delivery_type не совпадает с order_flow. Сейчас order_flow считается более надёжным.',
       raw_record
FROM staging.orders_current
WHERE delivery_type IS NOT NULL
  AND LOWER(delivery_type) NOT LIKE '%' || LOWER(order_flow) || '%'

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'warning', 'invalid_address_latitude',
       'address_latitude',
       address_latitude::TEXT,
       'Широта должна быть в диапазоне от -90 до 90.',
       raw_record
FROM staging.orders_current
WHERE address_latitude IS NOT NULL
  AND (address_latitude < -90 OR address_latitude > 90)

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'warning', 'invalid_address_longitude',
       'address_longitude',
       address_longitude::TEXT,
       'Долгота должна быть в диапазоне от -180 до 180.',
       raw_record
FROM staging.orders_current
WHERE address_longitude IS NOT NULL
  AND (address_longitude < -180 OR address_longitude > 180)

UNION ALL

SELECT raw_payload_id, record_index, dataset_name, source_file,
       order_flow, order_kind, order_id,
       'warning', 'skus_not_array',
       'skus',
       jsonb_typeof(skus),
       'skus ожидался как array. Строка не блокируется.',
       raw_record
FROM staging.orders_current
WHERE skus IS NOT NULL
  AND jsonb_typeof(skus) <> 'array';

CREATE TABLE quarantine.orders_current_quality AS
SELECT
    s.raw_payload_id,
    s.record_index,
    s.dataset_name,
    s.source_file,
    s.order_flow,
    s.order_kind,
    s.order_id,

    CASE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level = 'bad') > 0 THEN 'bad'
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level = 'partial') > 0 THEN 'partial'
        ELSE 'good'
    END AS quality_status,

    COALESCE(
        ARRAY_AGG(i.issue_code ORDER BY i.issue_code)
            FILTER (WHERE i.issue_level IN ('bad', 'partial')),
        ARRAY[]::TEXT[]
    ) AS quality_issues,

    COALESCE(
        ARRAY_AGG(i.issue_code ORDER BY i.issue_code)
            FILTER (WHERE i.issue_level = 'warning'),
        ARRAY[]::TEXT[]
    ) AS warning_issues,

    COALESCE(
        JSONB_AGG(
            JSONB_BUILD_OBJECT(
                'issue_level', i.issue_level,
                'issue_code', i.issue_code,
                'problem_field', i.problem_field,
                'problem_value', i.problem_value,
                'details', i.details
            )
            ORDER BY i.issue_level, i.issue_code
        ) FILTER (WHERE i.id IS NOT NULL),
        '[]'::JSONB
    ) AS issue_details,

    CASE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level = 'bad') > 0 THEN FALSE
        ELSE TRUE
    END AS can_load_to_core,

    CASE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level = 'bad') > 0 THEN FALSE
        ELSE TRUE
    END AS can_count_order,

    CASE
        WHEN COUNT(i.id) FILTER (
            WHERE i.issue_code IN ('missing_price', 'negative_price', 'final_price_greater_than_price')
        ) > 0 THEN FALSE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level = 'bad') > 0 THEN FALSE
        ELSE TRUE
    END AS can_count_revenue,

    CASE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_code = 'missing_created_at') > 0 THEN FALSE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level = 'bad') > 0 THEN FALSE
        ELSE TRUE
    END AS can_use_order_date,

    CASE
        WHEN COUNT(i.id) FILTER (WHERE i.issue_level IN ('bad', 'partial')) > 0 THEN FALSE
        ELSE TRUE
    END AS is_complete,

    NOW() AS checked_at
FROM staging.orders_current s
LEFT JOIN quarantine.orders_current_issues i
    ON i.raw_payload_id = s.raw_payload_id
   AND i.record_index = s.record_index
GROUP BY
    s.raw_payload_id,
    s.record_index,
    s.dataset_name,
    s.source_file,
    s.order_flow,
    s.order_kind,
    s.order_id;

ALTER TABLE quarantine.orders_current_quality
ADD PRIMARY KEY (raw_payload_id, record_index);

CREATE OR REPLACE VIEW quarantine.v_orders_current_good AS
SELECT s.*, q.quality_status, q.quality_issues, q.warning_issues, q.issue_details,
       q.can_load_to_core, q.can_count_order, q.can_count_revenue,
       q.can_use_order_date, q.is_complete
FROM staging.orders_current s
JOIN quarantine.orders_current_quality q
  ON q.raw_payload_id = s.raw_payload_id
 AND q.record_index = s.record_index
WHERE q.quality_status = 'good';

CREATE OR REPLACE VIEW quarantine.v_orders_current_partial AS
SELECT s.*, q.quality_status, q.quality_issues, q.warning_issues, q.issue_details,
       q.can_load_to_core, q.can_count_order, q.can_count_revenue,
       q.can_use_order_date, q.is_complete
FROM staging.orders_current s
JOIN quarantine.orders_current_quality q
  ON q.raw_payload_id = s.raw_payload_id
 AND q.record_index = s.record_index
WHERE q.quality_status = 'partial';

CREATE OR REPLACE VIEW quarantine.v_orders_current_bad AS
SELECT s.*, q.quality_status, q.quality_issues, q.warning_issues, q.issue_details,
       q.can_load_to_core, q.can_count_order, q.can_count_revenue,
       q.can_use_order_date, q.is_complete
FROM staging.orders_current s
JOIN quarantine.orders_current_quality q
  ON q.raw_payload_id = s.raw_payload_id
 AND q.record_index = s.record_index
WHERE q.quality_status = 'bad';

CREATE OR REPLACE VIEW quarantine.v_orders_current_for_core AS
SELECT s.*, q.quality_status, q.quality_issues, q.warning_issues, q.issue_details,
       q.can_load_to_core, q.can_count_order, q.can_count_revenue,
       q.can_use_order_date, q.is_complete
FROM staging.orders_current s
JOIN quarantine.orders_current_quality q
  ON q.raw_payload_id = s.raw_payload_id
 AND q.record_index = s.record_index
WHERE q.can_load_to_core = TRUE;

COMMIT;
