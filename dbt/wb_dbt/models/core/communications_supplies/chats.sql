{{ config(materialized='table', schema='core', alias='chats', tags=['core', 'core_communications_supplies']) }}

with source as (

    select *
    from {{ ref('communications_chats_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(
            nullif(chat_id, ''),
            id::text,
            raw_payload_id::text || ':' || record_index::text
        ) as chat_natural_id,

        coalesce(
            nullif(order_uid, ''),
            nullif(srid, ''),
            nullif(rid, ''),
            order_id::text,
            raw_payload_id::text || ':' || record_index::text
        ) as order_natural_id,

        case
            when last_message_add_timestamp is null then null
            when last_message_add_timestamp > 9999999999
                then to_timestamp(last_message_add_timestamp / 1000.0)
            else to_timestamp(last_message_add_timestamp::double precision)
        end as last_message_at_normalized

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by chat_natural_id
            order by coalesce(last_message_at_normalized, created_at, loaded_at) desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

)

select
    md5(concat_ws('||', 'chat', chat_natural_id)) as chat_key,

    chat_natural_id,
    chat_id,
    id as source_chat_row_id,

    client_name,
    reply_sign,

    created_at as chat_created_at,
    last_message_at_normalized as last_message_at,
    last_message_add_timestamp as last_message_timestamp_raw,
    last_message_text,

    md5(concat_ws('||', 'order', order_natural_id)) as order_key,
    order_natural_id,
    order_id,
    order_uid,
    rid,
    srid,

    coalesce(nm_id, good_card_nm_id) as product_id,
    coalesce(nm_id, good_card_nm_id) as nm_id,
    chrt_id as product_variant_id,
    chrt_id,

    article,
    vendor_code,
    barcode::text as barcode_value,
    skus,

    good_card_nm_id,
    good_card_rid,
    good_card_size,
    good_card_price,
    good_card_price_currency,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
