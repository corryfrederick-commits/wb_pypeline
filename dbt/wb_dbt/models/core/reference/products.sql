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

),

real_rows as (

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

),

fact_product_keys as (

    select client_id, wb_account_id, product_id
    from {{ ref('order_items') }}
    where product_id is not null

    union

    select client_id, wb_account_id, product_id
    from {{ ref('report_order_events') }}
    where product_id is not null

    union

    select client_id, wb_account_id, product_id
    from {{ ref('report_sale_events') }}
    where product_id is not null

),

inferred_rows as (

    select * from real_rows where false

    union all

    select
        f.client_id,
        f.wb_account_id,
        f.product_id,
        f.product_id as nm_id,
        null as nm_id_2,
        null as imt_id,
        null as nm_uuid,

        null as vendor_code,
        null as article,
        null as brand,
        null as subject_id,
        null as subject_name,
        null as title,
        null as description,

        null as photos,
        null as tags,
        null as video,

        null as dimensions_height,
        null as dimensions_length,
        null as dimensions_width,
        null as dimensions_weight_brutto,
        null as dimensions_is_valid,

        null as kiz_marked,
        null as need_kiz,
        null as wholesale_enabled,
        null as wholesale_quantum,

        null as created_at,
        null as updated_at,

        'inferred' as source_system,
        'inferred_from_fact_keys' as source_dataset,
        md5(concat_ws('||', f.client_id, f.wb_account_id, 'inferred_product', f.product_id::text)) as source_row_id,
        null as raw_payload_id,
        null as record_index,
        null as source_loaded_at,
        now() as core_loaded_at

    from fact_product_keys f
    left join real_rows r
        on f.client_id = r.client_id
       and f.wb_account_id = r.wb_account_id
       and f.product_id = r.product_id
    where r.product_id is null

)

select * from real_rows
union all
select * from inferred_rows
