{{ config(materialized='table', schema='marts', alias='mart_business_daily', tags=['marts']) }}

with orders_daily as (

    select
        client_id,
        wb_account_id,
        order_date as business_date,

        sum(orders_count) as orders_count,
        sum(order_items_count) as order_items_count,
        sum(final_price_sum) as orders_final_price_sum,
        sum(converted_final_price_sum) as orders_converted_final_price_sum

    from {{ ref('mart_orders_daily') }}
    group by
        client_id,
        wb_account_id,
        order_date

),

sales_daily as (

    select
        client_id,
        wb_account_id,
        sale_date as business_date,

        sum(sales_count) as sales_count,
        sum(sale_events_count) as sale_events_count,
        sum(final_price_sum) as sales_final_price_sum,
        sum(converted_final_price_sum) as sales_converted_final_price_sum,
        sum(finished_price_sum) as sales_finished_price_sum,
        sum(for_pay_sum) as sales_for_pay_sum

    from {{ ref('mart_sales_daily') }}
    group by
        client_id,
        wb_account_id,
        sale_date

),

promotion_daily as (

    select
        client_id,
        wb_account_id,
        promotion_date as business_date,

        sum(views) as promotion_views,
        sum(clicks) as promotion_clicks,
        sum(add_to_basket_count) as promotion_add_to_basket_count,
        sum(orders_count) as promotion_orders_count,
        sum(spend_sum) as promotion_spend_sum,
        sum(order_sum_price) as promotion_order_sum_price,

        case
            when sum(views) = 0 then null
            else sum(clicks)::numeric / sum(views)
        end as promotion_ctr,

        case
            when sum(clicks) = 0 then null
            else sum(spend_sum)::numeric / sum(clicks)
        end as promotion_cpc

    from {{ ref('mart_promotion_daily') }}
    group by
        client_id,
        wb_account_id,
        promotion_date

),

stock_daily as (

    select
        client_id,
        wb_account_id,
        stock_snapshot_date as business_date,

        sum(quantity) as stock_quantity,
        sum(amount) as stock_amount,
        sum(in_way_to_client) as stock_in_way_to_client,
        sum(in_way_from_client) as stock_in_way_from_client,
        sum(total_stock_with_in_way) as stock_total_with_in_way

    from {{ ref('mart_stock_current') }}
    group by
        client_id,
        wb_account_id,
        stock_snapshot_date

),

calendar as (

    select client_id, wb_account_id, business_date from orders_daily
    union
    select client_id, wb_account_id, business_date from sales_daily
    union
    select client_id, wb_account_id, business_date from promotion_daily
    union
    select client_id, wb_account_id, business_date from stock_daily

)

select
    c.client_id,
    c.wb_account_id,

    md5(concat_ws(
        '||',
        c.client_id,
        c.wb_account_id,
        'mart_business_daily',
        c.business_date::text
    )) as mart_business_daily_key,

    c.business_date,

    coalesce(o.orders_count, 0) as orders_count,
    coalesce(o.order_items_count, 0) as order_items_count,
    coalesce(o.orders_final_price_sum, 0) as orders_final_price_sum,
    coalesce(o.orders_converted_final_price_sum, 0) as orders_converted_final_price_sum,

    coalesce(s.sales_count, 0) as sales_count,
    coalesce(s.sale_events_count, 0) as sale_events_count,
    coalesce(s.sales_final_price_sum, 0) as sales_final_price_sum,
    coalesce(s.sales_converted_final_price_sum, 0) as sales_converted_final_price_sum,
    coalesce(s.sales_finished_price_sum, 0) as sales_finished_price_sum,
    coalesce(s.sales_for_pay_sum, 0) as sales_for_pay_sum,

    coalesce(p.promotion_views, 0) as promotion_views,
    coalesce(p.promotion_clicks, 0) as promotion_clicks,
    coalesce(p.promotion_add_to_basket_count, 0) as promotion_add_to_basket_count,
    coalesce(p.promotion_orders_count, 0) as promotion_orders_count,
    coalesce(p.promotion_spend_sum, 0) as promotion_spend_sum,
    coalesce(p.promotion_order_sum_price, 0) as promotion_order_sum_price,
    p.promotion_ctr,
    p.promotion_cpc,

    coalesce(st.stock_quantity, 0) as stock_quantity,
    coalesce(st.stock_amount, 0) as stock_amount,
    coalesce(st.stock_in_way_to_client, 0) as stock_in_way_to_client,
    coalesce(st.stock_in_way_from_client, 0) as stock_in_way_from_client,
    coalesce(st.stock_total_with_in_way, 0) as stock_total_with_in_way,

    now() as mart_loaded_at

from calendar c
left join orders_daily o
    on o.client_id = c.client_id
   and o.wb_account_id = c.wb_account_id
   and o.business_date = c.business_date
left join sales_daily s
    on s.client_id = c.client_id
   and s.wb_account_id = c.wb_account_id
   and s.business_date = c.business_date
left join promotion_daily p
    on p.client_id = c.client_id
   and p.wb_account_id = c.wb_account_id
   and p.business_date = c.business_date
left join stock_daily st
    on st.client_id = c.client_id
   and st.wb_account_id = c.wb_account_id
   and st.business_date = c.business_date
