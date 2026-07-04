{{ config(materialized='table', schema='core', alias='order_items', tags=['core', 'core_operations', 'scd2']) }}

with source as (

    select *
    from {{ ref('orders_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(
            nullif(order_uid, ''),
            nullif(srid, ''),
            nullif(rid, ''),
            order_id::text,
            raw_payload_id::text || ':' || record_index::text
        ) as order_natural_id,

        concat_ws(
            '||',
            coalesce(
                nullif(order_uid, ''),
                nullif(srid, ''),
                nullif(rid, ''),
                order_id::text,
                raw_payload_id::text || ':' || record_index::text
            ),
            coalesce(chrt_id::text, ''),
            coalesce(nm_id::text, ''),
            coalesce(barcode::text, ''),
            coalesce(article, '')
        ) as order_item_natural_id
    from source

),

hashed as (

    select
        *,
        md5(concat_ws(
            '||',
            coalesce(order_id::text, ''),
            coalesce(rid, ''),
            coalesce(srid, ''),
            coalesce(order_uid, ''),
            coalesce(order_code, ''),
            coalesce(nm_id::text, ''),
            coalesce(chrt_id::text, ''),
            coalesce(article, ''),
            coalesce(barcode::text, ''),
            coalesce(skus::text, ''),
            coalesce(price::text, ''),
            coalesce(sale_price::text, ''),
            coalesce(final_price::text, ''),
            coalesce(converted_price::text, ''),
            coalesce(converted_final_price::text, ''),
            coalesce(currency_code::text, ''),
            coalesce(converted_currency_code::text, ''),
            coalesce(scan_price::text, ''),
            coalesce(warehouse_id::text, ''),
            coalesce(office_id::text, ''),
            coalesce(order_flow, ''),
            coalesce(order_kind, ''),
            coalesce(delivery_type, ''),
            coalesce(delivery_method, ''),
            coalesce(delivery_service, ''),
            coalesce(pay_mode, ''),
            coalesce(created_at::text, ''),
            coalesce(ddate::text, ''),
            coalesce(seller_date::text, '')
        )) as order_item_row_hash
    from prepared

),

ordered as (

    select
        *,
        lag(order_item_row_hash) over (
            partition by client_id, wb_account_id, order_item_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as previous_order_item_row_hash
    from hashed

),

version_starts as (

    select *
    from ordered
    where previous_order_item_row_hash is null
       or previous_order_item_row_hash <> order_item_row_hash

),

versions as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, order_item_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as version_number,

        loaded_at as valid_from,

        lead(loaded_at) over (
            partition by client_id, wb_account_id, order_item_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as valid_to
    from version_starts

)

select
    client_id,
    wb_account_id,

    md5(concat_ws('||', client_id, wb_account_id, 'order_item', order_item_natural_id)) as order_item_key,
    md5(concat_ws('||', client_id, wb_account_id, 'order_item_version', order_item_natural_id, version_number::text, order_item_row_hash)) as order_item_version_key,

    order_item_natural_id,
    order_item_row_hash,
    version_number,
    valid_from,
    valid_to,
    valid_to is null as is_current,

    md5(concat_ws('||', client_id, wb_account_id, 'order', order_natural_id)) as order_key,
    order_natural_id,

    order_id,
    rid,
    srid,
    order_uid,
    order_code,

    nm_id as product_id,
    nm_id,
    chrt_id as product_variant_id,
    chrt_id,

    article,
    barcode as barcode_value,
    skus,

    price,
    sale_price,
    final_price,
    converted_price,
    converted_final_price,
    currency_code,
    converted_currency_code,
    scan_price,

    warehouse_id,
    office_id,

    order_flow,
    order_kind,
    delivery_type,
    delivery_method,
    delivery_service,
    pay_mode,

    created_at as order_created_at,
    ddate as delivery_deadline_at,
    seller_date,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from versions
