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
    where dataset_name = 'report_orders'
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
        nullif(raw_record #>> '{article}', '') as article,
        staging.try_bigint(raw_record #>> '{barcode}') as barcode,
        nullif(raw_record #>> '{brand}', '') as brand,
        staging.try_timestamptz(raw_record #>> '{cancelDate}') as cancel_date,
        nullif(raw_record #>> '{category}', '') as category,
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        staging.try_bigint(raw_record #>> '{convertedCurrencyCode}') as converted_currency_code,
        staging.try_bigint(raw_record #>> '{convertedFinalPrice}') as converted_final_price,
        staging.try_bigint(raw_record #>> '{convertedPrice}') as converted_price,
        nullif(raw_record #>> '{countryName}', '') as country_name,
        staging.try_timestamptz(raw_record #>> '{createdAt}') as created_at,
        staging.try_bigint(raw_record #>> '{currencyCode}') as currency_code,
        staging.try_timestamptz(raw_record #>> '{date}') as date_value,
        staging.try_bigint(raw_record #>> '{discountPercent}') as discount_percent,
        staging.try_bigint(raw_record #>> '{finalPrice}') as final_price,
        staging.try_bigint(raw_record #>> '{finishedPrice}') as finished_price,
        nullif(raw_record #>> '{gNumber}', '') as g_number,
        staging.try_bigint(raw_record #>> '{id}') as id,
        staging.try_bigint(raw_record #>> '{incomeID}') as income_id,
        staging.try_bool(raw_record #>> '{isCancel}') as is_cancel,
        staging.try_bool(raw_record #>> '{isRealization}') as is_realization,
        staging.try_bool(raw_record #>> '{isSupply}') as is_supply,
        staging.try_timestamptz(raw_record #>> '{lastChangeDate}') as last_change_date,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id,
        nullif(raw_record #>> '{oblastOkrugName}', '') as oblast_okrug_name,
        staging.try_bigint(raw_record #>> '{officeId}') as office_id,
        staging.try_timestamptz(raw_record #>> '{operationDate}') as operation_date,
        staging.try_bigint(raw_record #>> '{orderId}') as order_id,
        nullif(raw_record #>> '{orderUid}', '') as order_uid,
        staging.try_bigint(raw_record #>> '{price}') as price,
        staging.try_bigint(raw_record #>> '{priceWithDisc}') as price_with_disc,
        nullif(raw_record #>> '{regionName}', '') as region_name,
        nullif(raw_record #>> '{rid}', '') as rid,
        staging.try_timestamptz(raw_record #>> '{saleDt}') as sale_dt,
        staging.try_bigint(raw_record #>> '{salePrice}') as sale_price,
        raw_record #> '{skus}' as skus,
        staging.try_numeric(raw_record #>> '{spp}') as spp,
        nullif(raw_record #>> '{srid}', '') as srid,
        nullif(raw_record #>> '{sticker}', '') as sticker,
        nullif(raw_record #>> '{subject}', '') as subject,
        nullif(raw_record #>> '{supplierArticle}', '') as supplier_article,
        nullif(raw_record #>> '{techSize}', '') as tech_size,
        staging.try_bigint(raw_record #>> '{totalPrice}') as total_price,
        nullif(raw_record #>> '{vendorCode}', '') as vendor_code,
        nullif(raw_record #>> '{warehouseAddress}', '') as warehouse_address,
        staging.try_bigint(raw_record #>> '{warehouseId}') as warehouse_id,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name,
        nullif(raw_record #>> '{warehouseType}', '') as warehouse_type
    from expanded

)

select *
from typed
