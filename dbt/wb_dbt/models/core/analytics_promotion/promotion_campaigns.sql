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
            partition by promotion_campaign_natural_id
            order by coalesce(timestamps_updated, timestamps_created, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    md5(concat_ws('||', 'promotion_campaign', promotion_campaign_natural_id)) as promotion_campaign_key,

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
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
