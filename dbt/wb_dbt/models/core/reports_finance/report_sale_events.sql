{{ config(materialized='table', schema='core', alias='report_sale_events', tags=['core', 'core_reports_finance', 'scd2']) }}

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
        ) as order_natural_id,

        coalesce(
            nullif(sale_id::text, ''),
            id::text,
            nullif(srid, ''),
            nullif(rid, ''),
            nullif(order_uid, ''),
            order_id::text,
            nullif(g_number, ''),
            raw_payload_id::text || ':' || record_index::text
        ) as sale_event_natural_id
    from source

),

hashed as (

    select
        *,
        md5(concat_ws(
            '||',
            coalesce(id::text, ''),
            coalesce(sale_id::text, ''),
            coalesce(order_id::text, ''),
            coalesce(order_uid, ''),
            coalesce(rid, ''),
            coalesce(srid, ''),
            coalesce(g_number, ''),
            coalesce(nm_id::text, ''),
            coalesce(chrt_id::text, ''),
            coalesce(article, ''),
            coalesce(supplier_article, ''),
            coalesce(vendor_code, ''),
            coalesce(barcode::text, ''),
            coalesce(skus::text, ''),
            coalesce(brand, ''),
            coalesce(subject, ''),
            coalesce(category, ''),
            coalesce(tech_size, ''),
            coalesce(warehouse_id::text, ''),
            coalesce(office_id::text, ''),
            coalesce(warehouse_name, ''),
            coalesce(warehouse_address, ''),
            coalesce(warehouse_type, ''),
            coalesce(operation_date::text, ''),
            coalesce(date_value::text, ''),
            coalesce(created_at::text, ''),
            coalesce(last_change_date::text, ''),
            coalesce(sale_dt::text, ''),
            coalesce(is_realization::text, ''),
            coalesce(is_supply::text, ''),
            coalesce(country_name, ''),
            coalesce(region_name, ''),
            coalesce(oblast_okrug_name, ''),
            coalesce(price::text, ''),
            coalesce(total_price::text, ''),
            coalesce(price_with_disc::text, ''),
            coalesce(sale_price::text, ''),
            coalesce(final_price::text, ''),
            coalesce(finished_price::text, ''),
            coalesce(converted_price::text, ''),
            coalesce(converted_final_price::text, ''),
            coalesce(currency_code::text, ''),
            coalesce(converted_currency_code::text, ''),
            coalesce(discount_percent::text, ''),
            coalesce(spp::text, ''),
            coalesce(for_pay::text, ''),
            coalesce(payment_sale_amount::text, ''),
            coalesce(income_id::text, ''),
            coalesce(sticker, '')
        )) as sale_event_row_hash
    from prepared

),

ordered as (

    select
        *,
        lag(sale_event_row_hash) over (
            partition by client_id, wb_account_id, sale_event_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as previous_sale_event_row_hash
    from hashed

),

version_starts as (

    select *
    from ordered
    where previous_sale_event_row_hash is null
       or previous_sale_event_row_hash <> sale_event_row_hash

),

versions as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, sale_event_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as version_number,

        loaded_at as valid_from,

        lead(loaded_at) over (
            partition by client_id, wb_account_id, sale_event_natural_id
            order by loaded_at, raw_payload_id, record_index
        ) as valid_to
    from version_starts

)

select
    client_id,
    wb_account_id,

    md5(concat_ws('||', client_id, wb_account_id, 'report_sale_event', sale_event_natural_id)) as report_sale_event_key,
    md5(concat_ws('||', client_id, wb_account_id, 'report_sale_event_version', sale_event_natural_id, version_number::text, sale_event_row_hash)) as report_sale_event_version_key,

    sale_event_natural_id,
    sale_event_row_hash,
    version_number,
    valid_from,
    valid_to,
    valid_to is null as is_current,

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

from versions
