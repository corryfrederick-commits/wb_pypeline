{{ config(materialized='table', schema='marts', alias='mart_sales_daily', tags=['marts']) }}

with sales as (

    select *
    from {{ ref('report_sale_events_current') }}

)

select
    client_id,
    wb_account_id,

    md5(concat_ws(
        '||',
        client_id,
        wb_account_id,
        'mart_sales_daily',
        operation_date::date::text,
        coalesce(product_id::text, ''),
        coalesce(product_variant_id::text, ''),
        coalesce(warehouse_key, ''),
        coalesce(country_name, ''),
        coalesce(region_name, '')
    )) as mart_sales_daily_key,

    operation_date::date as sale_date,

    product_id,
    product_variant_id,
    warehouse_key,
    warehouse_id,
    office_id,
    warehouse_name,

    brand,
    subject,
    category,
    vendor_code,
    article,
    barcode_value,

    country_name,
    region_name,
    oblast_okrug_name,

    count(*) as sale_events_count,
    count(distinct sale_id) as sales_count,
    count(distinct order_key) as orders_count,

    sum(coalesce(price, 0)) as price_sum,
    sum(coalesce(total_price, 0)) as total_price_sum,
    sum(coalesce(price_with_disc, 0)) as price_with_disc_sum,
    sum(coalesce(sale_price, 0)) as sale_price_sum,
    sum(coalesce(final_price, 0)) as final_price_sum,
    sum(coalesce(finished_price, 0)) as finished_price_sum,
    sum(coalesce(converted_price, 0)) as converted_price_sum,
    sum(coalesce(converted_final_price, 0)) as converted_final_price_sum,
    sum(coalesce(for_pay, 0)) as for_pay_sum,
    sum(coalesce(payment_sale_amount, 0)) as payment_sale_amount_sum,

    avg(discount_percent) as avg_discount_percent,
    avg(spp) as avg_spp,

    max(source_loaded_at) as source_loaded_at,
    now() as mart_loaded_at

from sales
where operation_date is not null
group by
    client_id,
    wb_account_id,
    operation_date::date,
    product_id,
    product_variant_id,
    warehouse_key,
    warehouse_id,
    office_id,
    warehouse_name,
    brand,
    subject,
    category,
    vendor_code,
    article,
    barcode_value,
    country_name,
    region_name,
    oblast_okrug_name
