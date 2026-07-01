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
    where dataset_name = 'items_cards'
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
            when jsonb_typeof(p.payload #> '{cards}') = 'array' then p.payload #> '{cards}'
            when jsonb_typeof(p.payload #> '{cards}') = 'object' then jsonb_build_array(p.payload #> '{cards}')
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
        nullif(raw_record #>> '{article}', '') as article,
        staging.try_bigint(raw_record #>> '{barcode}') as barcode,
        nullif(raw_record #>> '{brand}', '') as brand,
        raw_record #> '{characteristics}' as characteristics,
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        staging.try_timestamptz(raw_record #>> '{createdAt}') as created_at,
        nullif(raw_record #>> '{description}', '') as description,
        staging.try_bigint(raw_record #>> '{dimensions,height}') as dimensions_height,
        staging.try_bool(raw_record #>> '{dimensions,isValid}') as dimensions_is_valid,
        staging.try_bigint(raw_record #>> '{dimensions,length}') as dimensions_length,
        staging.try_numeric(raw_record #>> '{dimensions,weightBrutto}') as dimensions_weight_brutto,
        staging.try_bigint(raw_record #>> '{dimensions,width}') as dimensions_width,
        staging.try_bigint(raw_record #>> '{imtID}') as imt_id,
        staging.try_bool(raw_record #>> '{kizMarked}') as kiz_marked,
        staging.try_bool(raw_record #>> '{needKiz}') as need_kiz,
        staging.try_bigint(raw_record #>> '{nmID}') as nm_id,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id_2,
        nullif(raw_record #>> '{nmUUID}', '') as nm_uuid,
        raw_record #> '{photos}' as photos,
        raw_record #> '{sizes}' as sizes,
        raw_record #> '{skus}' as skus,
        staging.try_bigint(raw_record #>> '{subjectID}') as subject_id,
        nullif(raw_record #>> '{subjectName}', '') as subject_name,
        raw_record #> '{tags}' as tags,
        nullif(raw_record #>> '{title}', '') as title,
        staging.try_timestamptz(raw_record #>> '{updatedAt}') as updated_at,
        nullif(raw_record #>> '{vendorCode}', '') as vendor_code,
        nullif(raw_record #>> '{video}', '') as video,
        staging.try_bool(raw_record #>> '{wholesale,enabled}') as wholesale_enabled,
        staging.try_numeric(raw_record #>> '{wholesale,quantum}') as wholesale_quantum
    from expanded

)

select *
from typed
