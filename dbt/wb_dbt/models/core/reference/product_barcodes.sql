{{ config(materialized='table', schema='core', alias='product_barcodes', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_cards_cleaned') }}
    where can_load_to_cleaned = true

),

direct_barcodes as (

    select
        client_id,
        wb_account_id,
        raw_payload_id,
        record_index,
        source_system,
        dataset_name,
        loaded_at,

        coalesce(nm_id, nm_id_2) as product_id,
        chrt_id as product_variant_id,
        nullif(barcode::text, '') as barcode_value,
        'barcode' as barcode_source

    from source
    where nullif(barcode::text, '') is not null

),

sku_barcodes as (

    select
        s.client_id,
        s.wb_account_id,
        s.raw_payload_id,
        s.record_index,
        s.source_system,
        s.dataset_name,
        s.loaded_at,

        coalesce(s.nm_id, s.nm_id_2) as product_id,
        s.chrt_id as product_variant_id,
        nullif(x.sku, '') as barcode_value,
        'skus' as barcode_source

    from source s
    cross join lateral jsonb_array_elements_text(
        case
            when jsonb_typeof(s.skus) = 'array' then s.skus
            when jsonb_typeof(s.skus) = 'string' then jsonb_build_array(s.skus)
            else '[]'::jsonb
        end
    ) as x(sku)

    where nullif(x.sku, '') is not null

),

combined as (

    select * from direct_barcodes
    union all
    select * from sku_barcodes

),

prepared as (

    select *
    from combined
    where product_id is not null
      and product_variant_id is not null
      and barcode_value is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by
                client_id,
                wb_account_id,
                product_id,
                product_variant_id,
                barcode_value
            order by loaded_at desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    client_id,
    wb_account_id,

    md5(concat_ws(
        '||',
        client_id,
        wb_account_id,
        'product_barcode',
        product_id::text,
        product_variant_id::text,
        barcode_value
    )) as product_barcode_key,

    md5(concat_ws('||', client_id, wb_account_id, 'product', product_id::text)) as product_key,
    md5(concat_ws('||', client_id, wb_account_id, 'product_variant', product_variant_id::text)) as product_variant_key,

    product_id,
    product_variant_id,
    barcode_value,
    barcode_source,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws(
        '||',
        client_id,
        wb_account_id,
        raw_payload_id::text,
        record_index::text,
        barcode_value
    )) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
