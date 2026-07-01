{{ config(materialized='table', schema='core', alias='warehouses', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_warehouses_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(warehouse_id, office_id, id) as warehouse_natural_id
    from source
    where coalesce(warehouse_id, office_id, id) is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by warehouse_natural_id
            order by loaded_at desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    md5(concat_ws('||', 'warehouse', warehouse_natural_id::text)) as warehouse_key,

    warehouse_natural_id,
    warehouse_id,
    office_id,
    id as source_warehouse_id,

    coalesce(nullif(warehouse_name, ''), nullif(name, '')) as warehouse_name,
    nullif(name, '') as source_name,
    nullif(warehouse_address, '') as warehouse_address,

    delivery_type,
    cargo_type,
    is_deleting,
    is_processing,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
