{{ config(materialized='table', schema='core', alias='sellers', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('general_seller_info_cleaned') }}
    where can_load_to_cleaned = true

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, coalesce(
                nullif(sid, ''),
                nullif(tin, ''),
                raw_payload_id::text || ':' || record_index::text
            )
            order by loaded_at desc, raw_payload_id desc, record_index desc
        ) as rn
    from source

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'seller',
        coalesce(
            nullif(sid, ''),
            nullif(tin, ''),
            raw_payload_id::text || ':' || record_index::text
        )
    )) as seller_key,

    nullif(sid, '') as seller_sid,
    nullif(tin, '') as seller_tin,
    nullif(name, '') as seller_name,
    nullif(trade_mark, '') as trade_mark,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from deduplicated
where rn = 1
