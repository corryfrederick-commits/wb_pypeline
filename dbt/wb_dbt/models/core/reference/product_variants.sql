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
            partition by client_id, wb_account_id, chrt_id
            order by coalesce(updated_at, created_at, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from source

),

real_rows as (

    select
        client_id,
        wb_account_id,
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
        md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
        raw_payload_id,
        record_index,
        loaded_at as source_loaded_at,
        now() as core_loaded_at

    from deduplicated
    where rn = 1

),

fact_variant_keys_raw as (

    select client_id, wb_account_id, product_variant_id, product_id
    from {{ ref('order_items') }}
    where product_variant_id is not null

    union all

    select client_id, wb_account_id, product_variant_id, product_id
    from {{ ref('report_order_events') }}
    where product_variant_id is not null

    union all

    select client_id, wb_account_id, product_variant_id, product_id
    from {{ ref('report_sale_events') }}
    where product_variant_id is not null

),

fact_variant_keys as (

    select
        client_id,
        wb_account_id,
        product_variant_id,
        max(product_id) as product_id
    from fact_variant_keys_raw
    group by client_id, wb_account_id, product_variant_id

),

inferred_rows as (

    select * from real_rows where false

    union all

    select
        f.client_id,
        f.wb_account_id,
        f.product_variant_id,
        f.product_variant_id as chrt_id,
        f.product_id,
        f.product_id as nm_id,

        null as vendor_code,
        null as article,
        null as barcode,
        null as skus,

        null as characteristics,
        null as sizes,

        null as created_at,
        null as updated_at,

        'inferred' as source_system,
        'inferred_from_fact_keys' as source_dataset,
        md5(concat_ws('||', f.client_id, f.wb_account_id, 'inferred_product_variant', f.product_variant_id::text)) as source_row_id,
        null as raw_payload_id,
        null as record_index,
        null as source_loaded_at,
        now() as core_loaded_at

    from fact_variant_keys f
    left join real_rows r
        on f.client_id = r.client_id
       and f.wb_account_id = r.wb_account_id
       and f.product_variant_id = r.product_variant_id
    where r.product_variant_id is null

)

select * from real_rows
union all
select * from inferred_rows
