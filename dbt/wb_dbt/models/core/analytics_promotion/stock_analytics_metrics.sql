{{ config(materialized='table', schema='core', alias='stock_analytics_metrics', tags=['core', 'core_analytics_promotion']) }}

with source as (

    select *
    from {{ ref('analytics_stocks_cleaned') }}
    where can_load_to_cleaned = true

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'stock_analytics_metric',
        raw_payload_id::text,
        record_index::text
    )) as stock_analytics_metric_key,

    loaded_at::date as metric_snapshot_date,
    loaded_at as metric_snapshot_at,

    nm_id as product_id,
    nm_id,
    chrt_id as product_variant_id,
    chrt_id,

    warehouse_id,
    md5(concat_ws('||', client_id, wb_account_id, 'warehouse', warehouse_id::text)) as warehouse_key,
    warehouse_name,
    region_name,

    quantity,
    in_way_to_client,
    in_way_from_client,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
