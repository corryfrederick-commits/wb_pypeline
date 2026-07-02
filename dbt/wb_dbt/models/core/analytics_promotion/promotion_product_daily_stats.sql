{{ config(materialized='table', schema='core', alias='promotion_product_daily_stats', tags=['core', 'core_analytics_promotion']) }}

with source as (

    select *
    from {{ ref('promotion_fullstats_cleaned') }}
    where can_load_to_cleaned = true

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        client_id,
        wb_account_id,
        'promotion_product_daily_stat',
        raw_payload_id::text,
        record_index::text,
        root_index::text,
        day_index::text,
        app_index::text,
        nm_index::text
    )) as promotion_product_daily_stat_key,

    root_campaign_id as campaign_id,
    root_advert_id as advert_id,

    md5(concat_ws(
        '||',
        client_id,
        wb_account_id,
        'promotion_campaign',
        coalesce(root_campaign_id::text, root_advert_id::text)
    )) as promotion_campaign_key,

    day_date as stat_date,
    loaded_at::date as stat_snapshot_date,
    loaded_at as stat_snapshot_at,

    app_app_type,

    coalesce(nm_nm_id, root_nm_id) as product_id,
    coalesce(nm_nm_id, root_nm_id) as nm_id,
    root_chrt_id as product_variant_id,
    root_chrt_id as chrt_id,

    nm_name,
    root_article,
    root_vendor_code,
    root_barcode::text as barcode_value,
    root_skus,

    root_views,
    root_clicks,
    root_ctr,
    root_cpc,
    root_cr,
    root_atbs,
    root_orders,
    root_sum,
    root_sum_price,
    root_shks,
    root_canceled,
    root_booster_stats,

    day_views,
    day_clicks,
    day_ctr,
    day_cpc,
    day_cr,
    day_atbs,
    day_orders,
    day_sum,
    day_sum_price,
    day_shks,
    day_canceled,

    app_views,
    app_clicks,
    app_ctr,
    app_cpc,
    app_cr,
    app_atbs,
    app_orders,
    app_sum,
    app_sum_price,
    app_shks,
    app_canceled,

    nm_views,
    nm_clicks,
    nm_ctr,
    nm_cpc,
    nm_cr,
    nm_atbs,
    nm_orders,
    nm_sum,
    nm_sum_price,
    nm_shks,
    nm_canceled,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    root_index,
    day_index,
    app_index,
    nm_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
