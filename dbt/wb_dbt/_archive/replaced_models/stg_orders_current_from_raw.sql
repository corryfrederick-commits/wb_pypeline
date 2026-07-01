{{ config(materialized='table') }}

with raw_payloads as (

    select
        id as raw_payload_id,
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

),

orders_expanded as (

    select
        p.raw_payload_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        (ord.idx - 1)::integer as record_index,
        ord.order_item as raw_record
    from raw_payloads p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload -> 'orders') = 'array'
                then p.payload -> 'orders'
            else '[]'::jsonb
        end
    ) with ordinality as ord(order_item, idx)

),

typed as (

    select
        raw_payload_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        record_index,

        case
            when dataset_name like 'orders_fbs_%' then 'fbs'
            when dataset_name like 'orders_dbs_%' then 'dbs'
            when dataset_name like 'orders_dbw_%' then 'dbw'
            when dataset_name like 'orders_pickup_%' then 'pickup'
            else null
        end as order_flow,

        case
            when dataset_name like '%_new' then 'new'
            when dataset_name like '%_current' then 'current'
            when dataset_name like '%_archive' then 'archive'
            when dataset_name like '%_completed' then 'completed'
            else null
        end as order_kind,

        case
            when dataset_name like '%_archive' then true
            else false
        end as is_archive,

        case
            when coalesce(raw_record ->> 'orderId', raw_record ->> 'orderID', raw_record ->> 'id') ~ '^-?[0-9]+$'
                then coalesce(raw_record ->> 'orderId', raw_record ->> 'orderID', raw_record ->> 'id')::bigint
            else null
        end as order_id,

        nullif(raw_record ->> 'orderUid', '') as order_uid,
        nullif(raw_record ->> 'rid', '') as rid,
        nullif(raw_record ->> 'srid', '') as srid,
        nullif(raw_record ->> 'orderCode', '') as order_code,

        case
            when nullif(raw_record ->> 'createdAt', '') is not null
                then nullif(raw_record ->> 'createdAt', '')::timestamptz
            when nullif(raw_record ->> 'created_at', '') is not null
                then nullif(raw_record ->> 'created_at', '')::timestamptz
            else null
        end as created_at,

        nullif(coalesce(
            raw_record ->> 'article',
            raw_record ->> 'supplierArticle',
            raw_record ->> 'vendorCode',
            raw_record #>> '{product,article}',
            raw_record #>> '{product,supplierArticle}',
            raw_record #>> '{product,vendorCode}'
        ), '') as article,

        case
            when coalesce(
                raw_record ->> 'nmId',
                raw_record ->> 'nmID',
                raw_record ->> 'nm_id',
                raw_record #>> '{product,nmId}',
                raw_record #>> '{product,nmID}',
                raw_record #>> '{product,nm_id}'
            ) ~ '^-?[0-9]+$'
            then coalesce(
                raw_record ->> 'nmId',
                raw_record ->> 'nmID',
                raw_record ->> 'nm_id',
                raw_record #>> '{product,nmId}',
                raw_record #>> '{product,nmID}',
                raw_record #>> '{product,nm_id}'
            )::bigint
            else null
        end as nm_id,

        case
            when coalesce(
                raw_record ->> 'chrtId',
                raw_record ->> 'chrtID',
                raw_record ->> 'chrt_id',
                raw_record #>> '{product,chrtId}',
                raw_record #>> '{product,chrtID}',
                raw_record #>> '{product,chrt_id}'
            ) ~ '^-?[0-9]+$'
            then coalesce(
                raw_record ->> 'chrtId',
                raw_record ->> 'chrtID',
                raw_record ->> 'chrt_id',
                raw_record #>> '{product,chrtId}',
                raw_record #>> '{product,chrtID}',
                raw_record #>> '{product,chrt_id}'
            )::bigint
            else null
        end as chrt_id,

        nullif(coalesce(
            raw_record ->> 'barcode',
            raw_record ->> 'barCode',
            raw_record ->> 'sku',
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

        case
            when coalesce(
                raw_record ->> 'warehouseId',
                raw_record ->> 'warehouseID',
                raw_record ->> 'warehouse_id'
            ) ~ '^-?[0-9]+$'
            then coalesce(
                raw_record ->> 'warehouseId',
                raw_record ->> 'warehouseID',
                raw_record ->> 'warehouse_id'
            )::bigint
            else null
        end as warehouse_id,

        case
            when coalesce(
                raw_record ->> 'officeId',
                raw_record ->> 'officeID',
                raw_record ->> 'office_id'
            ) ~ '^-?[0-9]+$'
            then coalesce(
                raw_record ->> 'officeId',
                raw_record ->> 'officeID',
                raw_record ->> 'office_id'
            )::bigint
            else null
        end as office_id,

        nullif(raw_record ->> 'deliveryType', '') as delivery_type,
        nullif(raw_record ->> 'deliveryMethod', '') as delivery_method,
        nullif(raw_record ->> 'deliveryService', '') as delivery_service,
        nullif(raw_record ->> 'payMode', '') as pay_mode,
        nullif(raw_record ->> 'currencyCode', '') as currency_code,

        case
            when coalesce(raw_record ->> 'price', raw_record #>> '{priceInfo,price}') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then coalesce(raw_record ->> 'price', raw_record #>> '{priceInfo,price}')::numeric
            else null
        end as price,

        case
            when coalesce(raw_record ->> 'salePrice', raw_record ->> 'sale_price') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then coalesce(raw_record ->> 'salePrice', raw_record ->> 'sale_price')::numeric
            else null
        end as sale_price,

        case
            when coalesce(raw_record ->> 'finalPrice', raw_record ->> 'final_price') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then coalesce(raw_record ->> 'finalPrice', raw_record ->> 'final_price')::numeric
            else null
        end as final_price,

        case
            when coalesce(raw_record ->> 'convertedPrice', raw_record #>> '{priceInfo,convertedPrice}') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then coalesce(raw_record ->> 'convertedPrice', raw_record #>> '{priceInfo,convertedPrice}')::numeric
            else null
        end as converted_price,

        case
            when coalesce(raw_record ->> 'convertedFinalPrice', raw_record ->> 'converted_final_price') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then coalesce(raw_record ->> 'convertedFinalPrice', raw_record ->> 'converted_final_price')::numeric
            else null
        end as converted_final_price,

        raw_record

    from orders_expanded

)

select *
from typed
