{{ config(materialized='table', schema='marts', alias='mart_products_catalog', tags=['marts']) }}

with products as (

    select *
    from {{ ref('products') }}

),

variants as (

    select *
    from {{ ref('product_variants') }}

),

barcodes as (

    select
        client_id,
        wb_account_id,
        product_id,
        product_variant_id,
        string_agg(distinct barcode_value, ', ' order by barcode_value) as barcode_values,
        count(distinct barcode_value) as barcode_count
    from {{ ref('product_barcodes') }}
    group by
        client_id,
        wb_account_id,
        product_id,
        product_variant_id

)

select
    p.client_id,
    p.wb_account_id,

    md5(concat_ws(
        '||',
        p.client_id,
        p.wb_account_id,
        'mart_products_catalog',
        p.product_id::text,
        coalesce(v.product_variant_id::text, 'no_variant')
    )) as mart_products_catalog_key,

    p.product_id,
    p.nm_id,
    p.imt_id,
    p.nm_uuid,

    v.product_variant_id,
    v.chrt_id,

    p.vendor_code as product_vendor_code,
    v.vendor_code as variant_vendor_code,
    coalesce(v.vendor_code, p.vendor_code) as vendor_code,

    p.article as product_article,
    v.article as variant_article,
    coalesce(v.article, p.article) as article,

    p.brand,
    p.subject_id,
    p.subject_name,
    p.title,
    p.description,

    b.barcode_values,
    b.barcode_count,

    p.photos,
    p.tags,
    p.video,

    p.dimensions_height,
    p.dimensions_length,
    p.dimensions_width,
    p.dimensions_weight_brutto,
    p.dimensions_is_valid,

    p.kiz_marked,
    p.need_kiz,
    p.wholesale_enabled,
    p.wholesale_quantum,

    p.created_at as product_created_at,
    p.updated_at as product_updated_at,
    v.created_at as variant_created_at,
    v.updated_at as variant_updated_at,

    greatest(
        coalesce(p.source_loaded_at, '1900-01-01'::timestamptz),
        coalesce(v.source_loaded_at, '1900-01-01'::timestamptz)
    ) as source_loaded_at,

    now() as mart_loaded_at

from products p
left join variants v
    on v.client_id = p.client_id
   and v.wb_account_id = p.wb_account_id
   and v.product_id = p.product_id
left join barcodes b
    on b.client_id = p.client_id
   and b.wb_account_id = p.wb_account_id
   and b.product_id = p.product_id
   and b.product_variant_id = v.product_variant_id
