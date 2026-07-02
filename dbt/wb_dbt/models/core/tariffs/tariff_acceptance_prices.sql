{{ config(materialized='table', schema='core', alias='tariff_acceptance_prices', tags=['core', 'core_tariffs']) }}

with source as (

    select *
    from {{ ref('tariffs_acceptance_cleaned') }}
    where can_load_to_cleaned = true

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'tariff_acceptance_price',
        raw_payload_id::text,
        record_index::text
    )) as tariff_acceptance_price_key,

    loaded_at::date as tariff_snapshot_date,
    loaded_at as tariff_snapshot_at,

    date_value as tariff_date,

    warehouse_id,

    case
        when warehouse_id is not null
            then md5(concat_ws('||', client_id, wb_account_id, 'warehouse', warehouse_id::text))
        else null
    end as warehouse_key,

    warehouse_name,

    box_type_id,
    coefficient,
    allow_unload,
    is_sorting_center,

    delivery_base_liter,
    delivery_additional_liter,
    delivery_coef,

    storage_base_liter,
    storage_additional_liter,
    storage_coef,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
