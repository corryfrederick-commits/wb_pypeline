{{ config(materialized='table', schema='core', alias='orders', tags=['core', 'core_operations']) }}

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

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, order_natural_id
            order by coalesce(created_at, seller_date, ddate, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    client_id,
    wb_account_id,
    md5(concat_ws('||', client_id, wb_account_id, 'order', order_natural_id)) as order_key,

    order_natural_id,
    order_id,
    rid,
    srid,
    order_uid,
    order_code,

    order_flow,
    order_kind,

    created_at as order_created_at,
    ddate as delivery_deadline_at,
    seller_date,
    inserted_at,

    delivery_type,
    delivery_method,
    delivery_service,
    pay_mode,

    warehouse_id,
    office_id,
    warehouse_address,

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

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
