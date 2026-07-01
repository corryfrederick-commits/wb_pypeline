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
    where dataset_name = 'communications_chats'
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
            when jsonb_typeof(p.payload #> '{result}') = 'array' then p.payload #> '{result}'
            when jsonb_typeof(p.payload #> '{result}') = 'object' then jsonb_build_array(p.payload #> '{result}')
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
        nullif(raw_record #>> '{chatID}', '') as chat_id,
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        nullif(raw_record #>> '{clientName}', '') as client_name,
        staging.try_timestamptz(raw_record #>> '{createdAt}') as created_at,
        staging.try_bigint(raw_record #>> '{goodCard,nmID}') as good_card_nm_id,
        staging.try_bigint(raw_record #>> '{goodCard,price}') as good_card_price,
        nullif(raw_record #>> '{goodCard,priceCurrency}', '') as good_card_price_currency,
        nullif(raw_record #>> '{goodCard,rid}', '') as good_card_rid,
        nullif(raw_record #>> '{goodCard,size}', '') as good_card_size,
        staging.try_bigint(raw_record #>> '{id}') as id,
        staging.try_bigint(raw_record #>> '{lastMessage,addTimestamp}') as last_message_add_timestamp,
        nullif(raw_record #>> '{lastMessage,text}', '') as last_message_text,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id,
        staging.try_bigint(raw_record #>> '{orderId}') as order_id,
        nullif(raw_record #>> '{orderUid}', '') as order_uid,
        nullif(raw_record #>> '{replySign}', '') as reply_sign,
        nullif(raw_record #>> '{rid}', '') as rid,
        raw_record #> '{skus}' as skus,
        nullif(raw_record #>> '{srid}', '') as srid,
        nullif(raw_record #>> '{vendorCode}', '') as vendor_code
    from expanded

)

select *
from typed
