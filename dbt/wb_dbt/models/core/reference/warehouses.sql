{{ config(materialized='table', schema='core', alias='warehouses', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_warehouses_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(id, warehouse_id, office_id) as warehouse_natural_id,
        coalesce(nullif(name, ''), nullif(warehouse_name, '')) as resolved_warehouse_name,
        coalesce(nullif(warehouse_address, ''), null) as resolved_warehouse_address
    from source
    where coalesce(id, warehouse_id, office_id) is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, warehouse_natural_id
            order by loaded_at desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    client_id,
    wb_account_id,
    md5(concat_ws('||', client_id, wb_account_id, 'warehouse', warehouse_natural_id::text)) as warehouse_key,

    warehouse_natural_id,
    warehouse_natural_id as warehouse_id,
    office_id,
    id as source_warehouse_id,

    resolved_warehouse_name as warehouse_name,
    nullif(name, '') as source_name,
    resolved_warehouse_address as warehouse_address,

    delivery_type,
    cargo_type,
    is_deleting,
    is_processing,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
