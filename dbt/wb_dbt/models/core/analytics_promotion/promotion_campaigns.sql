{{ config(materialized='table', schema='core', alias='promotion_campaigns', tags=['core', 'core_analytics_promotion']) }}

with source as (

    select *
    from {{ ref('promotion_campaigns_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(
            campaign_id::text,
            advert_id::text,
            id::text,
            raw_payload_id::text || ':' || record_index::text
        ) as promotion_campaign_natural_id
    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, promotion_campaign_natural_id
            order by coalesce(timestamps_updated, timestamps_created, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

),

real_rows as (

    select
        client_id,
        wb_account_id,
        md5(concat_ws('||', client_id, wb_account_id, 'promotion_campaign', promotion_campaign_natural_id)) as promotion_campaign_key,

        promotion_campaign_natural_id,

        campaign_id,
        advert_id,
        id as source_campaign_row_id,

        status,
        bid_type,

        settings_name,
        settings_payment_type,
        settings_placements_search,
        settings_placements_recommendations,
        nm_settings,

        nm_id as product_id,
        nm_id,
        chrt_id as product_variant_id,
        chrt_id,
        article,
        vendor_code,
        barcode::text as barcode_value,
        skus,

        timestamps_created as campaign_created_at,
        timestamps_started as campaign_started_at,
        timestamps_updated as campaign_updated_at,
        timestamps_deleted as campaign_deleted_at,

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

fact_campaign_keys as (

    select distinct
        client_id,
        wb_account_id,
        promotion_campaign_key
    from {{ ref('promotion_product_daily_stats') }}
    where promotion_campaign_key is not null

),

inferred_rows as (

    select * from real_rows where false

    union all

    select
        f.client_id,
        f.wb_account_id,
        f.promotion_campaign_key,

        'inferred:' || f.promotion_campaign_key::text as promotion_campaign_natural_id,

        null as campaign_id,
        null as advert_id,
        null as source_campaign_row_id,

        null as status,
        null as bid_type,

        null as settings_name,
        null as settings_payment_type,
        null as settings_placements_search,
        null as settings_placements_recommendations,
        null as nm_settings,

        null as product_id,
        null as nm_id,
        null as product_variant_id,
        null as chrt_id,
        null as article,
        null as vendor_code,
        null as barcode_value,
        null as skus,

        null as campaign_created_at,
        null as campaign_started_at,
        null as campaign_updated_at,
        null as campaign_deleted_at,

        'inferred' as source_system,
        'inferred_from_fact_keys' as source_dataset,
        md5(concat_ws('||', f.client_id, f.wb_account_id, 'inferred_promotion_campaign', f.promotion_campaign_key::text)) as source_row_id,
        null as raw_payload_id,
        null as record_index,
        null as source_loaded_at,
        now() as core_loaded_at

    from fact_campaign_keys f
    left join real_rows r
        on f.client_id = r.client_id
       and f.wb_account_id = r.wb_account_id
       and f.promotion_campaign_key = r.promotion_campaign_key
    where r.promotion_campaign_key is null

)

select * from real_rows
union all
select * from inferred_rows
