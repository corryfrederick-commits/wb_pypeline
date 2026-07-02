{{ config(materialized='table') }}

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
    where dataset_name in (
        'orders_fbs_new',
        'orders_fbs_current',
        'orders_fbs_archive',
        'orders_dbs_new',
        'orders_dbs_completed',
        'orders_dbw_new',
        'orders_dbw_completed',
        'orders_pickup_new'
    )
    order by
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

mapped_raw as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload,

        case
            when dataset_name like 'orders_fbs_%' then 'fbs'
            when dataset_name like 'orders_dbs_%' then 'dbs'
            when dataset_name like 'orders_dbw_%' then 'dbw'
            when dataset_name like 'orders_pickup_%' then 'pickup'
            else 'unknown'
        end as order_flow,

        case
            when dataset_name like '%_new' then 'new'
            when dataset_name like '%_current' then 'current'
            when dataset_name like '%_completed' then 'completed'
            when dataset_name like '%_archive' then 'archive'
            else 'unknown'
        end as order_kind
    from latest_raw

),

orders_expanded as (

    select
        r.raw_payload_id,
        r.client_id,
        r.wb_account_id,
        r.source_system,
        r.dataset_name,
        r.source_file,
        r.source_url,
        r.file_hash,
        r.loaded_at,
        r.order_flow,
        r.order_kind,

        -- ВАЖНО: как в старом staging.orders_current: ordinality начинается с 1.
        o.ordinality::integer as record_index,

        o.record as raw_record
    from mapped_raw r
    cross join lateral jsonb_array_elements(
        coalesce(r.payload -> 'orders', '[]'::jsonb)
    ) with ordinality as o(record, ordinality)

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

        order_flow,
        order_kind,

        staging.try_bigint(coalesce(
            raw_record #>> '{orderId}',
            raw_record #>> '{orderID}',
            raw_record #>> '{id}'
        )) as order_id,

        nullif(raw_record #>> '{rid}', '') as rid,
        nullif(raw_record #>> '{srid}', '') as srid,
        nullif(raw_record #>> '{orderUid}', '') as order_uid,
        nullif(raw_record #>> '{orderCode}', '') as order_code,

        staging.try_timestamptz(coalesce(
            raw_record #>> '{createdAt}',
            raw_record #>> '{created_at}'
        )) as created_at,

        staging.try_timestamptz(raw_record #>> '{ddate}') as ddate,
        staging.try_timestamptz(raw_record #>> '{sellerDate}') as seller_date,

        nullif(raw_record #>> '{deliveryType}', '') as delivery_type,
        nullif(raw_record #>> '{deliveryMethod}', '') as delivery_method,
        nullif(raw_record #>> '{deliveryService}', '') as delivery_service,
        nullif(raw_record #>> '{payMode}', '') as pay_mode,

        nullif(coalesce(
            raw_record #>> '{article}',
            raw_record #>> '{supplierArticle}',
            raw_record #>> '{vendorCode}',
            raw_record #>> '{product,article}',
            raw_record #>> '{product,supplierArticle}',
            raw_record #>> '{product,vendorCode}'
        ), '') as article,

        staging.try_bigint(coalesce(
            raw_record #>> '{nmId}',
            raw_record #>> '{nmID}',
            raw_record #>> '{nm_id}',
            raw_record #>> '{product,nmId}',
            raw_record #>> '{product,nmID}',
            raw_record #>> '{product,nm_id}'
        )) as nm_id,

        staging.try_bigint(coalesce(
            raw_record #>> '{chrtId}',
            raw_record #>> '{chrtID}',
            raw_record #>> '{chrt_id}',
            raw_record #>> '{product,chrtId}',
            raw_record #>> '{product,chrtID}',
            raw_record #>> '{product,chrt_id}'
        )) as chrt_id,

        nullif(coalesce(
            raw_record #>> '{barcode}',
            raw_record #>> '{barCode}',
            raw_record #>> '{sku}',
            raw_record #>> '{product,barcode}',
            raw_record #>> '{product,barCode}',
            raw_record #>> '{product,sku}'
        ), '') as barcode,

        coalesce(
            raw_record -> 'skus',
            raw_record -> 'sku',
            raw_record #> '{product,skus}',
            raw_record #> '{product,sku}'
        ) as skus,

        staging.try_numeric(coalesce(
            raw_record #>> '{price}',
            raw_record #>> '{priceInfo,price}'
        )) as price,

        staging.try_numeric(coalesce(
            raw_record #>> '{salePrice}',
            raw_record #>> '{sale_price}'
        )) as sale_price,

        staging.try_numeric(coalesce(
            raw_record #>> '{finalPrice}',
            raw_record #>> '{final_price}'
        )) as final_price,

        staging.try_numeric(coalesce(
            raw_record #>> '{convertedPrice}',
            raw_record #>> '{priceInfo,convertedPrice}'
        )) as converted_price,

        staging.try_numeric(coalesce(
            raw_record #>> '{convertedFinalPrice}',
            raw_record #>> '{converted_final_price}'
        )) as converted_final_price,

        staging.try_int(coalesce(
            raw_record #>> '{currencyCode}',
            raw_record #>> '{priceInfo,currencyCode}'
        )) as currency_code,

        staging.try_int(coalesce(
            raw_record #>> '{convertedCurrencyCode}',
            raw_record #>> '{priceInfo,convertedCurrencyCode}'
        )) as converted_currency_code,

        staging.try_numeric(raw_record #>> '{scanPrice}') as scan_price,

        staging.try_bigint(coalesce(
            raw_record #>> '{warehouseId}',
            raw_record #>> '{warehouseID}',
            raw_record #>> '{warehouse_id}'
        )) as warehouse_id,

        nullif(raw_record #>> '{warehouseAddress}', '') as warehouse_address,

        staging.try_bigint(coalesce(
            raw_record #>> '{officeId}',
            raw_record #>> '{officeID}',
            raw_record #>> '{office_id}'
        )) as office_id,

        nullif(raw_record #>> '{supplyId}', '') as supply_id,
        nullif(raw_record #>> '{groupId}', '') as group_id,

        nullif(raw_record #>> '{cargoType}', '') as cargo_type,
        nullif(raw_record #>> '{crossBorderType}', '') as cross_border_type,
        nullif(raw_record #>> '{colorCode}', '') as color_code,
        nullif(raw_record #>> '{comment}', '') as comment,

        staging.try_bool(raw_record #>> '{isZeroOrder}') as is_zero_order,

        staging.try_bool(coalesce(
            raw_record #>> '{options,isB2B}',
            raw_record #>> '{options,isB2b}',
            raw_record #>> '{options,is_b2b}',
            raw_record #>> '{options,b2b}',
            raw_record #>> '{isB2B}',
            raw_record #>> '{isB2b}',
            raw_record #>> '{is_b2b}',
            raw_record #>> '{b2b}'
        )) as is_b2b,

        case
            when dataset_name like '%_archive' then true
            else false
        end as is_archive,

        nullif(raw_record #>> '{address,fullAddress}', '') as address_full,
        staging.try_numeric(raw_record #>> '{address,latitude}') as address_latitude,
        staging.try_numeric(raw_record #>> '{address,longitude}') as address_longitude,

        raw_record,

        now() as inserted_at

    from orders_expanded

)

select *
from typed
