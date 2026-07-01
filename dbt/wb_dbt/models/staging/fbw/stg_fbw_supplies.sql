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
    where dataset_name = 'fbw_supplies'
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
            when jsonb_typeof(p.payload) = 'array' then p.payload
            when jsonb_typeof(p.payload) = 'object' then jsonb_build_array(p.payload)
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
        nullif(raw_record #>> '{boxTypeID}', '') as box_type_id,
        staging.try_timestamptz(raw_record #>> '{createDate}') as create_date,
        staging.try_timestamptz(raw_record #>> '{factDate}') as fact_date,
        staging.try_bool(raw_record #>> '{isBoxOnPallet}') as is_box_on_pallet,
        nullif(raw_record #>> '{phone}', '') as phone,
        staging.try_bigint(raw_record #>> '{preorderID}') as preorder_id,
        staging.try_bigint(raw_record #>> '{statusID}') as status_id,
        staging.try_timestamptz(raw_record #>> '{supplyDate}') as supply_date,
        staging.try_bigint(raw_record #>> '{supplyID}') as supply_id,
        staging.try_timestamptz(raw_record #>> '{updatedDate}') as updated_date
    from expanded

)

select *
from typed
