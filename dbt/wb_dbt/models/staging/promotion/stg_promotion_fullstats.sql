{{ config(materialized='table', schema='staging', tags=['nested_staging', 'auto_staging']) }}

with latest_raw as (

    select distinct on (source_system, dataset_name, source_file)
        id as raw_payload_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload
    from {{ source('quarantine', 'v_raw_payloads_schema_passed') }}
    where dataset_name = 'promotion_fullstats'
    order by
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded_root as (

    select
        p.raw_payload_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        r.ordinality::integer as root_index,
        r.raw_root
    from latest_raw p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload) = 'array' then p.payload
            when jsonb_typeof(p.payload) = 'object' then jsonb_build_array(p.payload)
            else '[]'::jsonb
        end
    ) with ordinality as r(raw_root, ordinality)

),

expanded_days as (

    select
        r.raw_payload_id,
        r.source_system,
        r.dataset_name,
        r.source_file,
        r.source_url,
        r.file_hash,
        r.loaded_at,
        r.root_index,
        d.ordinality::integer as day_index,
        r.raw_root,
        d.raw_day
    from expanded_root r
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(r.raw_root -> 'days') = 'array' then r.raw_root -> 'days'
            when jsonb_typeof(r.raw_root -> 'days') = 'object' then jsonb_build_array(r.raw_root -> 'days')
            else '[]'::jsonb
        end
    ) with ordinality as d(raw_day, ordinality)

),

expanded_apps as (

    select
        d.raw_payload_id,
        d.source_system,
        d.dataset_name,
        d.source_file,
        d.source_url,
        d.file_hash,
        d.loaded_at,
        d.root_index,
        d.day_index,
        a.ordinality::integer as app_index,
        d.raw_root,
        d.raw_day,
        a.raw_app
    from expanded_days d
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(d.raw_day -> 'apps') = 'array' then d.raw_day -> 'apps'
            when jsonb_typeof(d.raw_day -> 'apps') = 'object' then jsonb_build_array(d.raw_day -> 'apps')
            else '[]'::jsonb
        end
    ) with ordinality as a(raw_app, ordinality)

),

expanded_nms as (

    select
        a.raw_payload_id,
        a.source_system,
        a.dataset_name,
        a.source_file,
        a.source_url,
        a.file_hash,
        a.loaded_at,
        a.root_index,
        a.day_index,
        a.app_index,
        n.ordinality::integer as nm_index,
        a.raw_root,
        a.raw_day,
        a.raw_app,
        n.raw_nm
    from expanded_apps a
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(a.raw_app -> 'nms') = 'array' then a.raw_app -> 'nms'
            when jsonb_typeof(a.raw_app -> 'nms') = 'object' then jsonb_build_array(a.raw_app -> 'nms')
            else '[]'::jsonb
        end
    ) with ordinality as n(raw_nm, ordinality)

),

typed as (

    select
        raw_payload_id,
        (
            row_number() over (
                partition by raw_payload_id
                order by root_index, day_index, app_index, nm_index
            )
        )::integer as record_index,
        root_index,
        day_index,
        app_index,
        nm_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_root,
        raw_day,
        raw_app,
        raw_nm,
        raw_nm as raw_record,
        staging.try_bigint(raw_root #>> '{advertId}') as root_advert_id,
        nullif(raw_root #>> '{article}', '') as root_article,
        staging.try_bigint(raw_root #>> '{atbs}') as root_atbs,
        staging.try_bigint(raw_root #>> '{barcode}') as root_barcode,
        raw_root #> '{boosterStats}' as root_booster_stats,
        staging.try_bigint(raw_root #>> '{campaignId}') as root_campaign_id,
        staging.try_bigint(raw_root #>> '{canceled}') as root_canceled,
        staging.try_bigint(raw_root #>> '{chrtId}') as root_chrt_id,
        staging.try_bigint(raw_root #>> '{clicks}') as root_clicks,
        staging.try_numeric(raw_root #>> '{cpc}') as root_cpc,
        staging.try_numeric(raw_root #>> '{cr}') as root_cr,
        staging.try_numeric(raw_root #>> '{ctr}') as root_ctr,
        staging.try_bigint(raw_root #>> '{nmId}') as root_nm_id,
        staging.try_bigint(raw_root #>> '{orders}') as root_orders,
        staging.try_bigint(raw_root #>> '{shks}') as root_shks,
        raw_root #> '{skus}' as root_skus,
        staging.try_bigint(raw_root #>> '{sum}') as root_sum,
        staging.try_bigint(raw_root #>> '{sum_price}') as root_sum_price,
        nullif(raw_root #>> '{vendorCode}', '') as root_vendor_code,
        staging.try_bigint(raw_root #>> '{views}') as root_views,
        staging.try_bigint(raw_day #>> '{atbs}') as day_atbs,
        staging.try_bigint(raw_day #>> '{canceled}') as day_canceled,
        staging.try_bigint(raw_day #>> '{clicks}') as day_clicks,
        staging.try_numeric(raw_day #>> '{cpc}') as day_cpc,
        staging.try_numeric(raw_day #>> '{cr}') as day_cr,
        staging.try_numeric(raw_day #>> '{ctr}') as day_ctr,
        staging.try_timestamptz(raw_day #>> '{date}') as day_date,
        staging.try_bigint(raw_day #>> '{orders}') as day_orders,
        staging.try_bigint(raw_day #>> '{shks}') as day_shks,
        staging.try_bigint(raw_day #>> '{sum}') as day_sum,
        staging.try_bigint(raw_day #>> '{sum_price}') as day_sum_price,
        staging.try_bigint(raw_day #>> '{views}') as day_views,
        staging.try_bigint(raw_app #>> '{appType}') as app_app_type,
        staging.try_bigint(raw_app #>> '{atbs}') as app_atbs,
        staging.try_bigint(raw_app #>> '{canceled}') as app_canceled,
        staging.try_bigint(raw_app #>> '{clicks}') as app_clicks,
        staging.try_numeric(raw_app #>> '{cpc}') as app_cpc,
        staging.try_numeric(raw_app #>> '{cr}') as app_cr,
        staging.try_numeric(raw_app #>> '{ctr}') as app_ctr,
        staging.try_bigint(raw_app #>> '{orders}') as app_orders,
        staging.try_bigint(raw_app #>> '{shks}') as app_shks,
        staging.try_bigint(raw_app #>> '{sum}') as app_sum,
        staging.try_bigint(raw_app #>> '{sum_price}') as app_sum_price,
        staging.try_bigint(raw_app #>> '{views}') as app_views,
        staging.try_bigint(raw_nm #>> '{atbs}') as nm_atbs,
        staging.try_bigint(raw_nm #>> '{canceled}') as nm_canceled,
        staging.try_bigint(raw_nm #>> '{clicks}') as nm_clicks,
        staging.try_numeric(raw_nm #>> '{cpc}') as nm_cpc,
        staging.try_numeric(raw_nm #>> '{cr}') as nm_cr,
        staging.try_numeric(raw_nm #>> '{ctr}') as nm_ctr,
        nullif(raw_nm #>> '{name}', '') as nm_name,
        staging.try_bigint(raw_nm #>> '{nmId}') as nm_nm_id,
        staging.try_bigint(raw_nm #>> '{orders}') as nm_orders,
        staging.try_bigint(raw_nm #>> '{shks}') as nm_shks,
        staging.try_bigint(raw_nm #>> '{sum}') as nm_sum,
        staging.try_bigint(raw_nm #>> '{sum_price}') as nm_sum_price,
        staging.try_bigint(raw_nm #>> '{views}') as nm_views
    from expanded_nms

)

select *
from typed
