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
    where dataset_name = 'tariffs_box'
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
            when jsonb_typeof(p.payload #> '{response,data,warehouseList}') = 'array' then p.payload #> '{response,data,warehouseList}'
            when jsonb_typeof(p.payload #> '{response,data,warehouseList}') = 'object' then jsonb_build_array(p.payload #> '{response,data,warehouseList}')
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
        nullif(raw_record #>> '{boxDeliveryBase}', '') as box_delivery_base,
        nullif(raw_record #>> '{boxDeliveryCoefExpr}', '') as box_delivery_coef_expr,
        nullif(raw_record #>> '{boxDeliveryLiter}', '') as box_delivery_liter,
        nullif(raw_record #>> '{boxDeliveryMarketplaceBase}', '') as box_delivery_marketplace_base,
        nullif(raw_record #>> '{boxDeliveryMarketplaceCoefExpr}', '') as box_delivery_marketplace_coef_expr,
        nullif(raw_record #>> '{boxDeliveryMarketplaceLiter}', '') as box_delivery_marketplace_liter,
        nullif(raw_record #>> '{boxStorageBase}', '') as box_storage_base,
        nullif(raw_record #>> '{boxStorageCoefExpr}', '') as box_storage_coef_expr,
        nullif(raw_record #>> '{boxStorageLiter}', '') as box_storage_liter,
        nullif(raw_record #>> '{geoName}', '') as geo_name,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name
    from expanded

)

select *
from typed
