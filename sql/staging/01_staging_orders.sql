-- 01_staging_orders.sql
-- Новый staging orders под реалистичные mock API responses.
--
-- Теперь заказы лежат не как Order / OrderNew / ArchiveOrder,
-- а как массив:
--
-- payload -> 'orders' -> [ {...}, {...}, {...} ]

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE FUNCTION staging.try_bigint(v TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN NULLIF(TRIM(v), '')::BIGINT;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION staging.try_int(v TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN NULLIF(TRIM(v), '')::INTEGER;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION staging.try_numeric(v TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN REPLACE(NULLIF(TRIM(v), ''), ',', '.')::NUMERIC;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION staging.try_timestamptz(v TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN NULLIF(TRIM(v), '')::TIMESTAMPTZ;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION staging.try_bool(v TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    x TEXT;
BEGIN
    x := LOWER(NULLIF(TRIM(v), ''));

    IF x IN ('true', 't', '1', 'yes', 'y') THEN
        RETURN TRUE;
    END IF;

    IF x IN ('false', 'f', '0', 'no', 'n') THEN
        RETURN FALSE;
    END IF;

    RETURN NULL;
END;
$$;

CREATE TABLE IF NOT EXISTS staging.orders_current (
    raw_payload_id BIGINT NOT NULL,
    record_index INTEGER NOT NULL,

    dataset_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,

    order_flow TEXT NOT NULL,
    order_kind TEXT NOT NULL,

    order_id BIGINT,
    rid TEXT,
    order_uid TEXT,
    order_code TEXT,

    created_at TIMESTAMPTZ,
    ddate TIMESTAMPTZ,
    seller_date TIMESTAMPTZ,

    delivery_type TEXT,
    pay_mode TEXT,

    article TEXT,
    nm_id BIGINT,
    chrt_id BIGINT,

    price NUMERIC,
    sale_price NUMERIC,
    final_price NUMERIC,
    converted_price NUMERIC,
    converted_final_price NUMERIC,
    currency_code INTEGER,
    converted_currency_code INTEGER,
    scan_price NUMERIC,

    warehouse_id BIGINT,
    warehouse_address TEXT,
    office_id BIGINT,
    supply_id TEXT,
    group_id TEXT,

    cargo_type TEXT,
    cross_border_type TEXT,
    color_code TEXT,
    comment TEXT,

    is_zero_order BOOLEAN,
    is_b2b BOOLEAN,
    is_archive BOOLEAN NOT NULL DEFAULT FALSE,

    address_full TEXT,
    address_latitude NUMERIC,
    address_longitude NUMERIC,

    skus JSONB,
    raw_record JSONB NOT NULL,

    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (raw_payload_id, record_index)
);

TRUNCATE TABLE staging.orders_current;

WITH latest_raw AS (
    SELECT DISTINCT ON (source_system, dataset_name, source_file)
        id,
        source_system,
        dataset_name,
        source_file,
        loaded_at,
        payload
    FROM quarantine.v_raw_payloads_schema_passed
    WHERE dataset_name IN (
        'orders_fbs_new',
        'orders_fbs_current',
        'orders_fbs_archive',

        'orders_dbs_new',
        'orders_dbs_completed',

        'orders_dbw_new',
        'orders_dbw_completed',

        'orders_pickup_new'
    )
    ORDER BY
        source_system,
        dataset_name,
        source_file,
        loaded_at DESC,
        id DESC
),

mapped_raw AS (
    SELECT
        id,
        dataset_name,
        source_file,
        loaded_at,
        payload,

        CASE
            WHEN dataset_name LIKE 'orders_fbs_%' THEN 'fbs'
            WHEN dataset_name LIKE 'orders_dbs_%' THEN 'dbs'
            WHEN dataset_name LIKE 'orders_dbw_%' THEN 'dbw'
            WHEN dataset_name LIKE 'orders_pickup_%' THEN 'pickup'
            ELSE 'unknown'
        END AS order_flow,

        CASE
            WHEN dataset_name LIKE '%_new' THEN 'new'
            WHEN dataset_name LIKE '%_current' THEN 'current'
            WHEN dataset_name LIKE '%_completed' THEN 'completed'
            WHEN dataset_name LIKE '%_archive' THEN 'archive'
            ELSE 'unknown'
        END AS order_kind
    FROM latest_raw
),

orders_expanded AS (
    SELECT
        r.id AS raw_payload_id,
        (o.ordinality)::INTEGER AS record_index,

        r.dataset_name,
        r.source_file,
        r.loaded_at,

        r.order_flow,
        r.order_kind,

        o.record AS record
    FROM mapped_raw r
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(r.payload -> 'orders', '[]'::jsonb)
    ) WITH ORDINALITY AS o(record, ordinality)
)

INSERT INTO staging.orders_current (
    raw_payload_id,
    record_index,

    dataset_name,
    source_file,
    loaded_at,

    order_flow,
    order_kind,

    order_id,
    rid,
    order_uid,
    order_code,

    created_at,
    ddate,
    seller_date,

    delivery_type,
    pay_mode,

    article,
    nm_id,
    chrt_id,

    price,
    sale_price,
    final_price,
    converted_price,
    converted_final_price,
    currency_code,
    converted_currency_code,
    scan_price,

    warehouse_id,
    warehouse_address,
    office_id,
    supply_id,
    group_id,

    cargo_type,
    cross_border_type,
    color_code,
    comment,

    is_zero_order,
    is_b2b,
    is_archive,

    address_full,
    address_latitude,
    address_longitude,

    skus,
    raw_record
)
SELECT
    raw_payload_id,
    record_index,

    dataset_name,
    source_file,
    loaded_at,

    order_flow,
    order_kind,

    staging.try_bigint(COALESCE(
        record #>> '{id}',
        record #>> '{orderId}',
        record #>> '{orderID}'
    )) AS order_id,

    record #>> '{rid}' AS rid,

    record #>> '{orderUid}' AS order_uid,

    record #>> '{orderCode}' AS order_code,

    staging.try_timestamptz(record #>> '{createdAt}') AS created_at,

    staging.try_timestamptz(record #>> '{ddate}') AS ddate,

    staging.try_timestamptz(record #>> '{sellerDate}') AS seller_date,

    record #>> '{deliveryType}' AS delivery_type,

    record #>> '{payMode}' AS pay_mode,

    COALESCE(
        record #>> '{article}',
        record #>> '{product,article}'
    ) AS article,

    staging.try_bigint(COALESCE(
        record #>> '{nmId}',
        record #>> '{nmID}',
        record #>> '{product,nmId}'
    )) AS nm_id,

    staging.try_bigint(COALESCE(
        record #>> '{chrtId}',
        record #>> '{product,chrtId}'
    )) AS chrt_id,

    staging.try_numeric(COALESCE(
        record #>> '{price}',
        record #>> '{priceInfo,price}'
    )) AS price,

    staging.try_numeric(record #>> '{salePrice}') AS sale_price,

    staging.try_numeric(record #>> '{finalPrice}') AS final_price,

    staging.try_numeric(COALESCE(
        record #>> '{convertedPrice}',
        record #>> '{priceInfo,convertedPrice}'
    )) AS converted_price,

    staging.try_numeric(record #>> '{convertedFinalPrice}') AS converted_final_price,

    staging.try_int(COALESCE(
        record #>> '{currencyCode}',
        record #>> '{priceInfo,currencyCode}'
    )) AS currency_code,

    staging.try_int(COALESCE(
        record #>> '{convertedCurrencyCode}',
        record #>> '{priceInfo,convertedCurrencyCode}'
    )) AS converted_currency_code,

    staging.try_numeric(record #>> '{scanPrice}') AS scan_price,

    staging.try_bigint(record #>> '{warehouseId}') AS warehouse_id,

    record #>> '{warehouseAddress}' AS warehouse_address,

    staging.try_bigint(record #>> '{officeId}') AS office_id,

    record #>> '{supplyId}' AS supply_id,

    record #>> '{groupId}' AS group_id,

    record #>> '{cargoType}' AS cargo_type,

    record #>> '{crossBorderType}' AS cross_border_type,

    record #>> '{colorCode}' AS color_code,

    record #>> '{comment}' AS comment,

    staging.try_bool(record #>> '{isZeroOrder}') AS is_zero_order,

    staging.try_bool(COALESCE(
        record #>> '{options,isB2B}',
        record #>> '{options,isB2b}'
    )) AS is_b2b,

    CASE
        WHEN order_kind = 'archive' THEN TRUE
        ELSE FALSE
    END AS is_archive,

    record #>> '{address,fullAddress}' AS address_full,

    staging.try_numeric(record #>> '{address,latitude}') AS address_latitude,

    staging.try_numeric(record #>> '{address,longitude}') AS address_longitude,

    COALESCE(
        record #> '{skus}',
        record #> '{product,skus}'
    ) AS skus,

    record AS raw_record
FROM orders_expanded;

COMMIT;
