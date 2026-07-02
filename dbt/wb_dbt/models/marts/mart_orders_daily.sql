{{ config(materialized='table', schema='marts', alias='mart_orders_daily', tags=['marts']) }}

with order_items as (

    select *
    from {{ ref('order_items') }}

),

orders as (

    select *
    from {{ ref('orders') }}

),

enriched as (

    select
        oi.client_id,
        oi.wb_account_id,

        coalesce(o.order_created_at, oi.order_created_at)::date as order_date,

        oi.order_flow,
        oi.order_kind,
        oi.delivery_type,
        oi.delivery_method,
        oi.delivery_service,
        oi.pay_mode,

        oi.product_id,
        oi.product_variant_id,
        oi.warehouse_id,
        oi.office_id,

        oi.order_key,
        oi.order_item_key,

        oi.price,
        oi.sale_price,
        oi.final_price,
        oi.converted_price,
        oi.converted_final_price,

        oi.source_loaded_at

    from order_items oi
    left join orders o
        on o.client_id = oi.client_id
       and o.wb_account_id = oi.wb_account_id
       and o.order_key = oi.order_key

)

select
    client_id,
    wb_account_id,

    md5(concat_ws(
        '||',
        client_id,
        wb_account_id,
        'mart_orders_daily',
        order_date::text,
        coalesce(order_flow, ''),
        coalesce(order_kind, ''),
        coalesce(product_id::text, ''),
        coalesce(product_variant_id::text, ''),
        coalesce(warehouse_id::text, ''),
        coalesce(office_id::text, '')
    )) as mart_orders_daily_key,

    order_date,

    order_flow,
    order_kind,
    delivery_type,
    delivery_method,
    delivery_service,
    pay_mode,

    product_id,
    product_variant_id,
    warehouse_id,
    office_id,

    count(distinct order_key) as orders_count,
    count(*) as order_items_count,

    sum(coalesce(price, 0)) as price_sum,
    sum(coalesce(sale_price, 0)) as sale_price_sum,
    sum(coalesce(final_price, 0)) as final_price_sum,
    sum(coalesce(converted_price, 0)) as converted_price_sum,
    sum(coalesce(converted_final_price, 0)) as converted_final_price_sum,

    max(source_loaded_at) as source_loaded_at,
    now() as mart_loaded_at

from enriched
where order_date is not null
group by
    client_id,
    wb_account_id,
    order_date,
    order_flow,
    order_kind,
    delivery_type,
    delivery_method,
    delivery_service,
    pay_mode,
    product_id,
    product_variant_id,
    warehouse_id,
    office_id
