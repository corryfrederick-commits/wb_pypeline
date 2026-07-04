{{ config(materialized='table', schema='core', alias='orders', tags=['core', 'core_operations', 'scd2']) }}

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
            coalesce(order_flow, ''),
            coalesce(order_kind, ''),
            coalesce(created_at::text, ''),
            coalesce(ddate::text, ''),
            coalesce(seller_date::text, ''),
            coalesce(inserted_at::text, ''),
            coalesce(delivery_type, ''),
            coalesce(delivery_method, ''),
            coalesce(delivery_service, ''),
            coalesce(pay_mode, ''),
            coalesce(warehouse_id::text, ''),
            coalesce(office_id::text, ''),
            coalesce(warehouse_address, ''),
            coalesce(supply_id::text, ''),
            coalesce(group_id::text, ''),
            coalesce(cargo_type::text, ''),
            coalesce(cross_border_type, ''),
            coalesce(color_code, ''),
            coalesce(comment, ''),
            coalesce(is_zero_order::text, ''),
            coalesce(is_b2b::text, ''),
            coalesce(is_archive::text, ''),
            coalesce(address_full, ''),
            coalesce(address_latitude::text, ''),
            coalesce(address_longitude::text, '')
        )) as order_row_hash
    from prepared

),

ordered as (

    select
        *,
        lag(order_row_hash) over (
            partition by client_id, wb_account_id, order_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as previous_order_row_hash
    from hashed

),

version_starts as (

    select *
    from ordered
    where previous_order_row_hash is null
       or previous_order_row_hash <> order_row_hash

),

versions as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, order_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as version_number,

        loaded_at as valid_from,

        lead(loaded_at) over (
            partition by client_id, wb_account_id, order_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as valid_to
    from version_starts

)

select
    client_id,
    wb_account_id,

    md5(concat_ws('||', client_id, wb_account_id, 'order', order_natural_id)) as order_key,
    md5(concat_ws('||', client_id, wb_account_id, 'order_version', order_natural_id, version_number::text, order_row_hash)) as order_version_key,

    order_natural_id,
    order_row_hash,
    version_number,
    valid_from,
    valid_to,
    valid_to is null as is_current,

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

from versions
