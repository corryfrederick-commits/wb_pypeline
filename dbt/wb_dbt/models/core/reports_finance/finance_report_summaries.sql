{{ config(materialized='table', schema='core', alias='finance_report_summaries', tags=['core', 'core_reports_finance']) }}

with source as (

    select *
    from {{ ref('finance_sales_reports_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(
            nullif(order_uid, ''),
            nullif(srid, ''),
            nullif(rid, ''),
            order_id::text,
            raw_payload_id::text || ':' || record_index::text
        ) as order_natural_id
    from source

)

select
    md5(concat_ws(
        '||',
        'finance_report_summary',
        raw_payload_id::text,
        record_index::text
    )) as finance_report_summary_key,

    id as source_finance_report_row_id,
    report_id,
    report_type,
    payment_schedule,
    seller_finance_name,

    date_from,
    date_to,
    operation_date,
    create_date,
    created_at,
    sale_dt,

    md5(concat_ws('||', 'order', order_natural_id)) as order_key,
    order_natural_id,
    order_id,
    order_uid,
    rid,
    srid,

    nm_id as product_id,
    nm_id,
    chrt_id as product_variant_id,
    chrt_id,
    article,
    vendor_code,
    barcode::text as barcode_value,
    skus,

    warehouse_id,
    office_id,
    md5(concat_ws('||', 'warehouse', coalesce(warehouse_id, office_id)::text)) as warehouse_key,
    warehouse_name,
    warehouse_address,

    currency,
    currency_code,
    converted_currency_code,

    price,
    sale_price,
    final_price,
    converted_price,
    converted_final_price,

    retail_amount_sum,
    for_pay_sum,
    bank_payment_sum,
    additional_payment_sum,
    cashback_amount_sum,
    cashback_commission_change_sum,
    cashback_discount_sum,
    deduction_sum,
    delivery_service_sum,
    paid_acceptance_sum,
    paid_storage_sum,
    penalty_sum,

    avg_sale_percent,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from prepared
