{{ config(materialized='table', schema='core', alias='product_barcodes', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_cards_cleaned') }}
    where can_load_to_cleaned = true

),

primary_barcodes as (

    select
        raw_payload_id,
        record_index,
        source_system,
        dataset_name,
        loaded_at,
        nm_id,
        chrt_id,
        nullif(barcode::text, '') as barcode_value,
        'barcode' as barcode_source,
        1 as source_priority
    from source
    where barcode is not null

),

sku_barcodes as (

    select
        s.raw_payload_id,
        s.record_index,
        s.source_system,
        s.dataset_name,
        s.loaded_at,
        s.nm_id,
        s.chrt_id,
        nullif(sku.value, '') as barcode_value,
        'skus' as barcode_source,
        2 as source_priority
    from source as s
    cross join lateral jsonb_array_elements_text(
        case
            when jsonb_typeof(s.skus) = 'array' then s.skus
            else '[]'::jsonb
        end
    ) as sku(value)

),

unioned as (

    select * from primary_barcodes
    union all
    select * from sku_barcodes

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by
                coalesce(nm_id::text, 'unknown'),
                coalesce(chrt_id::text, 'unknown'),
                barcode_value
            order by source_priority, loaded_at desc, raw_payload_id desc, record_index desc
        ) as rn
    from unioned
    where barcode_value is not null

)

select
    md5(concat_ws(
        '||',
        'product_barcode',
        coalesce(nm_id::text, 'unknown'),
        coalesce(chrt_id::text, 'unknown'),
        barcode_value
    )) as product_barcode_key,

    nm_id as product_id,
    nm_id,
    chrt_id as product_variant_id,
    chrt_id,
    barcode_value,
    barcode_source,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
