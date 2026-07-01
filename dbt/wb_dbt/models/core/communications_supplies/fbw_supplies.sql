{{ config(materialized='table', schema='core', alias='fbw_supplies', tags=['core', 'core_communications_supplies']) }}

with source as (

    select *
    from {{ ref('fbw_supplies_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(
            nullif(supply_id::text, ''),
            preorder_id::text,
            raw_payload_id::text || ':' || record_index::text
        ) as fbw_supply_natural_id
    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by fbw_supply_natural_id
            order by coalesce(updated_date, fact_date, supply_date, create_date, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    md5(concat_ws('||', 'fbw_supply', fbw_supply_natural_id)) as fbw_supply_key,

    fbw_supply_natural_id,
    supply_id,
    preorder_id,

    status_id,
    box_type_id,
    is_box_on_pallet,

    create_date as supply_created_at,
    supply_date,
    fact_date,
    updated_date as supply_updated_at,

    phone,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
