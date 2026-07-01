{{ config(materialized='table', schema='core', alias='finance_transactions', tags=['core', 'core_reports_finance']) }}

with source as (

    select *
    from {{ ref('finance_sales_reports_detailed_cleaned') }}
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
        'finance_transaction',
        raw_payload_id::text,
        record_index::text
    )) as finance_transaction_key,

    id as source_finance_transaction_id,
    rrd_id,
    report_id,
    report_type,
    payment_schedule,

    operation_date,
    create_date,
    created_at,
    date_from,
    date_to,
    sale_dt,
    order_dt,
    rr_date,

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
    sku,
    skus,
    brand_name,
    subject_name,
    title,
    tech_size,

    warehouse_id,
    office_id,
    md5(concat_ws('||', 'warehouse', coalesce(warehouse_id, office_id)::text)) as warehouse_key,
    warehouse_name,
    warehouse_address,
    office_name,

    country,

    currency,
    currency_code,
    converted_currency_code,

    price,
    retail_price,
    retail_price_with_disc,
    sale_price,
    final_price,
    converted_price,
    converted_final_price,

    quantity,
    delivery_amount,
    return_amount,

    for_pay,
    retail_amount,
    acquiring_fee,
    acquiring_percent,
    acquiring_bank,

    commission_percent,
    kvw,
    kvw_base,
    dlv_prc,
    ppvz_reward,
    ppvz_sales_commission,
    vw,
    vw_nds,

    additional_payment,
    cashback_amount,
    cashback_commission_change,
    cashback_discount,
    deduction,
    paid_acceptance,
    paid_storage,
    penalty,
    delivery_service,
    payment_processing,

    loyalty_discount,
    product_discount_for_report,
    sale_percent,
    sale_price_affiliated_discount_prc,
    sale_price_promocode_discount_prc,
    sale_price_wholesale_discount_prc,
    seller_promo,
    seller_promo_discount,
    seller_promo_id,
    spp,

    seller_oper_name,
    doc_type_name,
    bonus_type_name,

    gi_id,
    shk_id,
    sticker_id,
    trbx_id,
    declaration_number,
    kiz,

    is_b2b,
    is_kgvp_v2,
    srv_dbs,

    ppvz_office_id,
    ppvz_office_name,
    ppvz_supplier_inn,
    ppvz_supplier_name,

    rebill_logistic_cost,
    rebill_logistic_org,

    installment_cofinancing_amount,
    agency_vat,
    sup_rating_up,

    fix_tariff_date_from,
    fix_tariff_date_to,

    article_substitution,
    loyalty_id,
    uuid_promocode,
    wibes_discount_percent,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from prepared
