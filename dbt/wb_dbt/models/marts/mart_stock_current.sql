{{ config(materialized='table', schema='marts', alias='mart_stock_current', tags=['marts']) }}

with stock as (

    select *
    from {{ ref('stock_balances') }}

),

latest_stock as (

    select
        *,
        row_number() over (
            partition by
                client_id,
                wb_account_id,
                product_id,
                product_variant_id,
                warehouse_key
            order by
                stock_snapshot_at desc,
                source_loaded_at desc,
                raw_payload_id desc,
                record_index desc
        ) as rn
    from stock

),

products as (

    select *
    from {{ ref('products') }}

),

variants as (

    select *
    from {{ ref('product_variants') }}

),

warehouses as (

    select *
    from {{ ref('warehouses') }}

)

select
    s.client_id,
    s.wb_account_id,

    md5(concat_ws(
        '||',
        s.client_id,
        s.wb_account_id,
        'mart_stock_current',
        s.product_id::text,
        s.product_variant_id::text,
        s.warehouse_key
    )) as mart_stock_current_key,

    s.stock_snapshot_date,
    s.stock_snapshot_at,

    s.product_id,
    s.nm_id,
    s.product_variant_id,
    s.chrt_id,

    p.brand,
    p.subject_id,
    p.subject_name,
    p.title,
    coalesce(v.vendor_code, p.vendor_code, s.vendor_code) as vendor_code,
    coalesce(v.article, p.article, s.article) as article,
    s.barcode_value,

    s.warehouse_key,
    s.warehouse_natural_id,
    s.warehouse_id,
    s.office_id,
    coalesce(w.warehouse_name, s.warehouse_name) as warehouse_name,
    coalesce(w.warehouse_address, s.warehouse_address) as warehouse_address,

    coalesce(s.quantity, 0) as quantity,
    coalesce(s.amount, 0) as amount,
    coalesce(s.in_way_to_client, 0) as in_way_to_client,
    coalesce(s.in_way_from_client, 0) as in_way_from_client,

    coalesce(s.quantity, 0)
        + coalesce(s.in_way_to_client, 0)
        + coalesce(s.in_way_from_client, 0) as total_stock_with_in_way,

    s.source_loaded_at,
    now() as mart_loaded_at

from latest_stock s
left join products p
    on p.client_id = s.client_id
   and p.wb_account_id = s.wb_account_id
   and p.product_id = s.product_id
left join variants v
    on v.client_id = s.client_id
   and v.wb_account_id = s.wb_account_id
   and v.product_variant_id = s.product_variant_id
left join warehouses w
    on w.client_id = s.client_id
   and w.wb_account_id = s.wb_account_id
   and w.warehouse_key = s.warehouse_key
where s.rn = 1
