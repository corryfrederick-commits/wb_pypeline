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
    where dataset_name = 'tariffs_commission'
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
            when jsonb_typeof(p.payload #> '{report}') = 'array' then p.payload #> '{report}'
            when jsonb_typeof(p.payload #> '{report}') = 'object' then jsonb_build_array(p.payload #> '{report}')
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
        staging.try_numeric(raw_record #>> '{kgvpBooking}') as kgvp_booking,
        staging.try_numeric(raw_record #>> '{kgvpMarketplace}') as kgvp_marketplace,
        staging.try_numeric(raw_record #>> '{kgvpPickup}') as kgvp_pickup,
        staging.try_numeric(raw_record #>> '{kgvpSupplier}') as kgvp_supplier,
        staging.try_numeric(raw_record #>> '{kgvpSupplierExpress}') as kgvp_supplier_express,
        staging.try_numeric(raw_record #>> '{paidStorageKgvp}') as paid_storage_kgvp,
        staging.try_bigint(raw_record #>> '{parentID}') as parent_id,
        nullif(raw_record #>> '{parentName}', '') as parent_name,
        staging.try_bigint(raw_record #>> '{subjectID}') as subject_id,
        nullif(raw_record #>> '{subjectName}', '') as subject_name
    from expanded

)

select *
from typed
