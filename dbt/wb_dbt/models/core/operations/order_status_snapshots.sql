{{ config(materialized='table', schema='core', alias='order_status_snapshots', tags=['core', 'core_operations']) }}

with fbs as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        loaded_at,
        created_at,
        updated_at,
        id,
        order_id,
        order_uid,
        rid,
        srid,
        status,
        supplier_status,
        wb_status,
        is_cancellable,
        null::jsonb as errors
    from {{ ref('orders_fbs_statuses_cleaned') }}
    where can_load_to_cleaned = true

),

dbs as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        loaded_at,
        created_at,
        updated_at,
        id,
        order_id,
        order_uid,
        rid,
        srid,
        status,
        supplier_status,
        wb_status,
        null::boolean as is_cancellable,
        errors
    from {{ ref('orders_dbs_statuses_cleaned') }}
    where can_load_to_cleaned = true

),

dbw as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        loaded_at,
        created_at,
        updated_at,
        id,
        order_id,
        order_uid,
        rid,
        srid,
        status,
        supplier_status,
        wb_status,
        null::boolean as is_cancellable,
        null::jsonb as errors
    from {{ ref('orders_dbw_statuses_cleaned') }}
    where can_load_to_cleaned = true

),

pickup as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        loaded_at,
        created_at,
        updated_at,
        id,
        order_id,
        order_uid,
        rid,
        srid,
        status,
        supplier_status,
        wb_status,
        null::boolean as is_cancellable,
        errors
    from {{ ref('orders_pickup_statuses_cleaned') }}
    where can_load_to_cleaned = true

),

unioned as (

    select * from fbs
    union all
    select * from dbs
    union all
    select * from dbw
    union all
    select * from pickup

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

        case
            when dataset_name like '%fbs%' then 'fbs'
            when dataset_name like '%dbs%' then 'dbs'
            when dataset_name like '%dbw%' then 'dbw'
            when dataset_name like '%pickup%' then 'pickup'
            else 'unknown'
        end as order_flow_from_status

    from unioned

)

select
    md5(concat_ws(
        '||',
        'order_status_snapshot',
        dataset_name,
        raw_payload_id::text,
        record_index::text
    )) as order_status_snapshot_key,

    md5(concat_ws('||', client_id, wb_account_id, 'order', order_natural_id)) as order_key,
    order_natural_id,

    id as source_status_id,
    order_id,
    order_uid,
    rid,
    srid,

    order_flow_from_status as order_flow,
    dataset_name as source_status_dataset,

    status,
    supplier_status,
    wb_status,
    is_cancellable,
    errors,

    created_at as status_created_at,
    updated_at as status_updated_at,
    loaded_at as status_snapshot_at,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
        client_id,
        wb_account_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from prepared
