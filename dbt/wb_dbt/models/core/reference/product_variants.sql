{{ config(materialized='table', schema='core', alias='product_variants', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_cards_cleaned') }}
    where can_load_to_cleaned = true
      and chrt_id is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by chrt_id
            order by coalesce(updated_at, created_at, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from source

)

select
    chrt_id as product_variant_id,
    chrt_id,
    nm_id as product_id,
    nm_id,

    vendor_code,
    article,
    barcode,
    skus,

    characteristics,
    sizes,

    created_at,
    updated_at,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
