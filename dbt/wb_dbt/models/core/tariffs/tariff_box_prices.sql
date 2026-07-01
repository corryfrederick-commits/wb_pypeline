{{ config(materialized='table', schema='core', alias='tariff_box_prices', tags=['core', 'core_tariffs']) }}

with source as (

    select *
    from {{ ref('tariffs_box_cleaned') }}
    where can_load_to_cleaned = true

)

select
    md5(concat_ws(
        '||',
        'tariff_box_price',
        raw_payload_id::text,
        record_index::text
    )) as tariff_box_price_key,

    loaded_at::date as tariff_snapshot_date,
    loaded_at as tariff_snapshot_at,

    geo_name,
    warehouse_name,

    box_delivery_base,
    box_delivery_coef_expr,
    box_delivery_liter,

    box_delivery_marketplace_base,
    box_delivery_marketplace_coef_expr,
    box_delivery_marketplace_liter,

    box_storage_base,
    box_storage_coef_expr,
    box_storage_liter,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
