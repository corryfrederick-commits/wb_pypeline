{{ config(materialized='table', schema='core', alias='tariff_commissions', tags=['core', 'core_tariffs']) }}

with source as (

    select *
    from {{ ref('tariffs_commission_cleaned') }}
    where can_load_to_cleaned = true

)

select
    md5(concat_ws(
        '||',
        'tariff_commission',
        raw_payload_id::text,
        record_index::text
    )) as tariff_commission_key,

    loaded_at::date as tariff_snapshot_date,
    loaded_at as tariff_snapshot_at,

    parent_id,
    parent_name,
    subject_id,
    subject_name,

    kgvp_booking,
    kgvp_marketplace,
    kgvp_pickup,
    kgvp_supplier,
    kgvp_supplier_express,
    paid_storage_kgvp,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
