{{ config(materialized='table', schema='marts', alias='mart_promotion_daily', tags=['marts']) }}

with stats as (

    select *
    from {{ ref('promotion_product_daily_stats') }}

),

campaigns as (

    select *
    from {{ ref('promotion_campaigns') }}

),

base as (

    select
        s.client_id,
        s.wb_account_id,

        md5(concat_ws(
            '||',
            s.client_id,
            s.wb_account_id,
            'mart_promotion_daily',
            s.stat_date::date::text,
            s.promotion_campaign_key,
            coalesce(s.product_id::text, ''),
            coalesce(s.product_variant_id::text, ''),
            coalesce(s.app_app_type::text, '')
        )) as mart_promotion_daily_key,

        s.stat_date::date as promotion_date,
        s.stat_snapshot_date,
        s.stat_snapshot_at,

        s.promotion_campaign_key,
        s.campaign_id,
        s.advert_id,

        c.status as campaign_status,
        c.bid_type,
        c.settings_name,
        c.settings_payment_type,
        c.settings_placements_search,
        c.settings_placements_recommendations,

        s.app_app_type,

        s.product_id,
        s.product_variant_id,
        s.nm_id,
        s.chrt_id,
        s.nm_name,
        coalesce(c.vendor_code, s.root_vendor_code) as vendor_code,
        coalesce(c.article, s.root_article) as article,
        coalesce(c.barcode_value, s.barcode_value) as barcode_value,

        coalesce(s.nm_views, s.app_views, s.day_views, s.root_views, 0) as views,
        coalesce(s.nm_clicks, s.app_clicks, s.day_clicks, s.root_clicks, 0) as clicks,
        coalesce(s.nm_atbs, s.app_atbs, s.day_atbs, s.root_atbs, 0) as add_to_basket_count,
        coalesce(s.nm_orders, s.app_orders, s.day_orders, s.root_orders, 0) as orders_count,
        coalesce(s.nm_sum, s.app_sum, s.day_sum, s.root_sum, 0) as spend_sum,
        coalesce(s.nm_sum_price, s.app_sum_price, s.day_sum_price, s.root_sum_price, 0) as order_sum_price,
        coalesce(s.nm_shks, s.app_shks, s.day_shks, s.root_shks, 0) as shks_count,
        coalesce(s.nm_canceled, s.app_canceled, s.day_canceled, s.root_canceled, 0) as canceled_count,

        case
            when coalesce(s.nm_views, s.app_views, s.day_views, s.root_views, 0) = 0
                then null
            else
                coalesce(s.nm_clicks, s.app_clicks, s.day_clicks, s.root_clicks, 0)::numeric
                / coalesce(s.nm_views, s.app_views, s.day_views, s.root_views, 0)
        end as ctr_calculated,

        case
            when coalesce(s.nm_clicks, s.app_clicks, s.day_clicks, s.root_clicks, 0) = 0
                then null
            else
                coalesce(s.nm_sum, s.app_sum, s.day_sum, s.root_sum, 0)::numeric
                / coalesce(s.nm_clicks, s.app_clicks, s.day_clicks, s.root_clicks, 0)
        end as cpc_calculated,

        s.source_loaded_at,
        s.promotion_product_daily_stat_key,

        row_number() over (
            partition by
                s.client_id,
                s.wb_account_id,
                s.stat_date::date,
                s.promotion_campaign_key,
                s.product_id,
                s.product_variant_id,
                s.app_app_type
            order by
                s.stat_snapshot_at desc,
                s.source_loaded_at desc,
                s.promotion_product_daily_stat_key desc
        ) as rn

    from stats s
    left join campaigns c
        on c.client_id = s.client_id
       and c.wb_account_id = s.wb_account_id
       and c.promotion_campaign_key = s.promotion_campaign_key
    where s.stat_date is not null

)

select
    client_id,
    wb_account_id,
    mart_promotion_daily_key,

    promotion_date,
    stat_snapshot_date,
    stat_snapshot_at,

    promotion_campaign_key,
    campaign_id,
    advert_id,

    campaign_status,
    bid_type,
    settings_name,
    settings_payment_type,
    settings_placements_search,
    settings_placements_recommendations,

    app_app_type,

    product_id,
    product_variant_id,
    nm_id,
    chrt_id,
    nm_name,
    vendor_code,
    article,
    barcode_value,

    views,
    clicks,
    add_to_basket_count,
    orders_count,
    spend_sum,
    order_sum_price,
    shks_count,
    canceled_count,

    ctr_calculated,
    cpc_calculated,

    source_loaded_at,
    now() as mart_loaded_at

from base
where rn = 1
