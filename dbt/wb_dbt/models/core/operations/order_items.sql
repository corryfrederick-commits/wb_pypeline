{{ config(materialized='table', schema='core', alias='order_items', tags=['core', 'core_operations']) }}

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
        ) as order_natural_id
    from source

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'order_item',
        raw_payload_id::text,
        record_index::text
    )) as order_item_key,

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

from prepared
