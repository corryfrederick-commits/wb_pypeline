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
    where dataset_name = 'tariffs_acceptance'
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
        staging.try_bool(raw_record #>> '{allowUnload}') as allow_unload,
        staging.try_bigint(raw_record #>> '{boxTypeID}') as box_type_id,
        staging.try_numeric(raw_record #>> '{coefficient}') as coefficient,
        staging.try_timestamptz(raw_record #>> '{date}') as date_value,
        nullif(raw_record #>> '{deliveryAdditionalLiter}', '') as delivery_additional_liter,
        nullif(raw_record #>> '{deliveryBaseLiter}', '') as delivery_base_liter,
        nullif(raw_record #>> '{deliveryCoef}', '') as delivery_coef,
        staging.try_bool(raw_record #>> '{isSortingCenter}') as is_sorting_center,
        nullif(raw_record #>> '{storageAdditionalLiter}', '') as storage_additional_liter,
        nullif(raw_record #>> '{storageBaseLiter}', '') as storage_base_liter,
        nullif(raw_record #>> '{storageCoef}', '') as storage_coef,
        staging.try_bigint(raw_record #>> '{warehouseID}') as warehouse_id,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name
    from expanded

)

select *
from typed
