{{ config(materialized='table', schema='staging', tags=['auto_staging']) }}

with latest_raw as (

    select distinct on (client_id, wb_account_id, source_system, dataset_name, source_file)
        id as raw_payload_id,
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload
    from {{ source('quarantine', 'v_raw_payloads_schema_passed') }}
    where dataset_name = 'items_warehouses'
    order by
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded as (

    select
        p.raw_payload_id,
        p.client_id,
        p.wb_account_id,
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
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_record,
        staging.try_bigint(raw_record #>> '{cargoType}') as cargo_type,
        staging.try_bigint(raw_record #>> '{deliveryType}') as delivery_type,
        staging.try_bigint(raw_record #>> '{id}') as id,
        staging.try_bool(raw_record #>> '{isDeleting}') as is_deleting,
        staging.try_bool(raw_record #>> '{isProcessing}') as is_processing,
        nullif(raw_record #>> '{name}', '') as name,
        staging.try_bigint(raw_record #>> '{officeId}') as office_id,
        nullif(raw_record #>> '{warehouseAddress}', '') as warehouse_address,
        staging.try_bigint(raw_record #>> '{warehouseId}') as warehouse_id,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name
    from expanded

)

select *
from typed
