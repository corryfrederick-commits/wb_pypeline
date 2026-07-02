{{ config(materialized='table', schema='core', alias='products', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_cards_cleaned') }}
    where can_load_to_cleaned = true
      and nm_id is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, nm_id
            order by coalesce(updated_at, created_at, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from source

)

select
    client_id,
    wb_account_id,
    nm_id as product_id,
    nm_id,
    nm_id_2,
    imt_id,
    nm_uuid,

    vendor_code,
    article,
    brand,
    subject_id,
    subject_name,
    title,
    description,

    photos,
    tags,
    video,

    dimensions_height,
    dimensions_length,
    dimensions_width,
    dimensions_weight_brutto,
    dimensions_is_valid,

    kiz_marked,
    need_kiz,
    wholesale_enabled,
    wholesale_quantum,

    created_at,
    updated_at,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
