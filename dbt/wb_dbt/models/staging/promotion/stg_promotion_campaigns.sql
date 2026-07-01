{{ config(materialized='table', schema='staging', tags=['auto_staging']) }}

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
    where dataset_name = 'promotion_campaigns'
    order by
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded as (

    select
        p.raw_payload_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        x.ordinality::integer as record_index,
        x.raw_record
    from latest_raw p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload #> '{adverts}') = 'array' then p.payload #> '{adverts}'
            when jsonb_typeof(p.payload #> '{adverts}') = 'object' then jsonb_build_array(p.payload #> '{adverts}')
            else '[]'::jsonb
        end
    ) with ordinality as x(raw_record, ordinality)

),

typed as (

    select
        raw_payload_id,
        record_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_record,
        staging.try_bigint(raw_record #>> '{advertId}') as advert_id,
        nullif(raw_record #>> '{article}', '') as article,
        staging.try_bigint(raw_record #>> '{barcode}') as barcode,
        nullif(raw_record #>> '{bid_type}', '') as bid_type,
        staging.try_bigint(raw_record #>> '{campaignId}') as campaign_id,
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        staging.try_bigint(raw_record #>> '{id}') as id,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id,
        raw_record #> '{nm_settings}' as nm_settings,
        nullif(raw_record #>> '{settings,name}', '') as settings_name,
        nullif(raw_record #>> '{settings,payment_type}', '') as settings_payment_type,
        staging.try_bool(raw_record #>> '{settings,placements,recommendations}') as settings_placements_recommendations,
        staging.try_bool(raw_record #>> '{settings,placements,search}') as settings_placements_search,
        raw_record #> '{skus}' as skus,
        staging.try_bigint(raw_record #>> '{status}') as status,
        staging.try_timestamptz(raw_record #>> '{timestamps,created}') as timestamps_created,
        staging.try_timestamptz(raw_record #>> '{timestamps,deleted}') as timestamps_deleted,
        staging.try_timestamptz(raw_record #>> '{timestamps,started}') as timestamps_started,
        staging.try_timestamptz(raw_record #>> '{timestamps,updated}') as timestamps_updated,
        nullif(raw_record #>> '{vendorCode}', '') as vendor_code
    from expanded

)

select *
from typed
