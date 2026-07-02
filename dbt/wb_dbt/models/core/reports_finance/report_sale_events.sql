{{ config(materialized='table', schema='core', alias='report_sale_events', tags=['core', 'core_reports_finance']) }}

with source as (

    select *
    from {{ ref('report_sales_cleaned') }}
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
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'report_sale_event',
        raw_payload_id::text,
        record_index::text
    )) as report_sale_event_key,

    id as source_report_sale_id,
    sale_id,

    md5(concat_ws('||', client_id, wb_account_id, 'order', order_natural_id)) as order_key,
    order_natural_id,
    order_id,
    order_uid,
    rid,
    srid,
    g_number,

    nm_id as product_id,
    nm_id,
    chrt_id as product_variant_id,
    chrt_id,
    article,
    supplier_article,
    vendor_code,
    barcode::text as barcode_value,
    skus,
    brand,
    subject,
    category,
    tech_size,

    warehouse_id,
    office_id,
    md5(concat_ws('||', client_id, wb_account_id, 'warehouse', coalesce(warehouse_id, office_id)::text)) as warehouse_key,
    warehouse_name,
    warehouse_address,
    warehouse_type,

    operation_date,
    date_value,
    created_at,
    last_change_date,
    sale_dt,

    is_realization,
    is_supply,

    country_name,
    region_name,
    oblast_okrug_name,

    price,
    total_price,
    price_with_disc,
    sale_price,
    final_price,
    finished_price,
    converted_price,
    converted_final_price,
    currency_code,
    converted_currency_code,
    discount_percent,
    spp,
    for_pay,
    payment_sale_amount,

    income_id,
    sticker,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from prepared
