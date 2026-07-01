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
    where dataset_name = 'analytics_stocks'
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
            when jsonb_typeof(p.payload #> '{data,items}') = 'array' then p.payload #> '{data,items}'
            when jsonb_typeof(p.payload #> '{data,items}') = 'object' then jsonb_build_array(p.payload #> '{data,items}')
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
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        staging.try_bigint(raw_record #>> '{inWayFromClient}') as in_way_from_client,
        staging.try_bigint(raw_record #>> '{inWayToClient}') as in_way_to_client,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id,
        staging.try_bigint(raw_record #>> '{quantity}') as quantity,
        nullif(raw_record #>> '{regionName}', '') as region_name,
        staging.try_bigint(raw_record #>> '{warehouseId}') as warehouse_id,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name
    from expanded

)

select *
from typed
