{{ config(materialized='table', schema='core', alias='finance_balances', tags=['core', 'core_reports_finance']) }}

with source as (

    select *
    from {{ ref('finance_balance_cleaned') }}
    where can_load_to_cleaned = true

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'finance_balance',
        raw_payload_id::text,
        record_index::text
    )) as finance_balance_key,

    loaded_at::date as balance_snapshot_date,
    loaded_at as balance_snapshot_at,

    currency,
    current as current_balance,
    for_withdraw as available_for_withdraw,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
