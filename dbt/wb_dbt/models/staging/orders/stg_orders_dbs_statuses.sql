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
    where dataset_name = 'orders_dbs_statuses'
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
            when jsonb_typeof(p.payload #> '{orders}') = 'array' then p.payload #> '{orders}'
            when jsonb_typeof(p.payload #> '{orders}') = 'object' then jsonb_build_array(p.payload #> '{orders}')
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
        staging.try_timestamptz(raw_record #>> '{createdAt}') as created_at,
        raw_record #> '{errors}' as errors,
        staging.try_bigint(raw_record #>> '{id}') as id,
        staging.try_bigint(raw_record #>> '{orderId}') as order_id,
        nullif(raw_record #>> '{orderUid}', '') as order_uid,
        nullif(raw_record #>> '{rid}', '') as rid,
        nullif(raw_record #>> '{srid}', '') as srid,
        nullif(raw_record #>> '{status}', '') as status,
        nullif(raw_record #>> '{supplierStatus}', '') as supplier_status,
        staging.try_timestamptz(raw_record #>> '{updatedAt}') as updated_at,
        nullif(raw_record #>> '{wbStatus}', '') as wb_status
    from expanded

)

select *
from typed
