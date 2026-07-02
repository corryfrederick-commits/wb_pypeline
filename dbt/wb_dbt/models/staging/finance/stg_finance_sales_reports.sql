{{ config(materialized='table', schema='staging', tags=['auto_staging']) }}

with latest_raw as (

    select distinct on (client_id, wb_account_id, source_system, dataset_name, source_file)
        id as raw_payload_id,
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload
    from {{ source('quarantine', 'v_raw_payloads_schema_passed') }}
    where dataset_name = 'finance_sales_reports'
    order by
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded as (

    select
        p.raw_payload_id,
        p.client_id,
        p.wb_account_id,
        p.source_system,
        p.dataset_name,
        p.source_file,
        p.source_url,
        p.file_hash,
        p.loaded_at,
        x.ordinality::integer as record_index,
        x.raw_record
    from latest_raw p
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(p.payload) = 'array' then p.payload
            when jsonb_typeof(p.payload) = 'object' then jsonb_build_array(p.payload)
            else '[]'::jsonb
        end
    ) with ordinality as x(raw_record, ordinality)

),

typed as (

    select
        raw_payload_id,
        client_id,
        wb_account_id,
        record_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_record,
        nullif(raw_record #>> '{additionalPaymentSum}', '') as additional_payment_sum,
        nullif(raw_record #>> '{article}', '') as article,
        staging.try_numeric(raw_record #>> '{avgSalePercent}') as avg_sale_percent,
        nullif(raw_record #>> '{bankPaymentSum}', '') as bank_payment_sum,
        staging.try_bigint(raw_record #>> '{barcode}') as barcode,
        nullif(raw_record #>> '{cashbackAmountSum}', '') as cashback_amount_sum,
        nullif(raw_record #>> '{cashbackCommissionChangeSum}', '') as cashback_commission_change_sum,
        nullif(raw_record #>> '{cashbackDiscountSum}', '') as cashback_discount_sum,
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        staging.try_bigint(raw_record #>> '{convertedCurrencyCode}') as converted_currency_code,
        staging.try_bigint(raw_record #>> '{convertedFinalPrice}') as converted_final_price,
        staging.try_bigint(raw_record #>> '{convertedPrice}') as converted_price,
        staging.try_timestamptz(raw_record #>> '{createDate}') as create_date,
        staging.try_timestamptz(raw_record #>> '{createdAt}') as created_at,
        nullif(raw_record #>> '{currency}', '') as currency,
        staging.try_bigint(raw_record #>> '{currencyCode}') as currency_code,
        staging.try_timestamptz(raw_record #>> '{dateFrom}') as date_from,
        staging.try_timestamptz(raw_record #>> '{dateTo}') as date_to,
        nullif(raw_record #>> '{deductionSum}', '') as deduction_sum,
        nullif(raw_record #>> '{deliveryServiceSum}', '') as delivery_service_sum,
        staging.try_bigint(raw_record #>> '{finalPrice}') as final_price,
        nullif(raw_record #>> '{forPaySum}', '') as for_pay_sum,
        staging.try_bigint(raw_record #>> '{id}') as id,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id,
        staging.try_bigint(raw_record #>> '{officeId}') as office_id,
        staging.try_timestamptz(raw_record #>> '{operationDate}') as operation_date,
        staging.try_bigint(raw_record #>> '{orderId}') as order_id,
        nullif(raw_record #>> '{orderUid}', '') as order_uid,
        nullif(raw_record #>> '{paidAcceptanceSum}', '') as paid_acceptance_sum,
        nullif(raw_record #>> '{paidStorageSum}', '') as paid_storage_sum,
        nullif(raw_record #>> '{paymentSchedule}', '') as payment_schedule,
        nullif(raw_record #>> '{penaltySum}', '') as penalty_sum,
        staging.try_bigint(raw_record #>> '{price}') as price,
        staging.try_bigint(raw_record #>> '{reportId}') as report_id,
        staging.try_bigint(raw_record #>> '{reportType}') as report_type,
        nullif(raw_record #>> '{retailAmountSum}', '') as retail_amount_sum,
        nullif(raw_record #>> '{rid}', '') as rid,
        staging.try_timestamptz(raw_record #>> '{saleDt}') as sale_dt,
        staging.try_bigint(raw_record #>> '{salePrice}') as sale_price,
        nullif(raw_record #>> '{sellerFinanceName}', '') as seller_finance_name,
        raw_record #> '{skus}' as skus,
        nullif(raw_record #>> '{srid}', '') as srid,
        nullif(raw_record #>> '{vendorCode}', '') as vendor_code,
        nullif(raw_record #>> '{warehouseAddress}', '') as warehouse_address,
        staging.try_bigint(raw_record #>> '{warehouseId}') as warehouse_id,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name
    from expanded

)

select *
from typed
