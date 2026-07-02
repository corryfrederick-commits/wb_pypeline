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
    where dataset_name = 'finance_sales_reports_detailed'
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
        nullif(raw_record #>> '{acquiringBank}', '') as acquiring_bank,
        nullif(raw_record #>> '{acquiringFee}', '') as acquiring_fee,
        staging.try_numeric(raw_record #>> '{acquiringPercent}') as acquiring_percent,
        nullif(raw_record #>> '{additionalPayment}', '') as additional_payment,
        staging.try_numeric(raw_record #>> '{agencyVat}') as agency_vat,
        nullif(raw_record #>> '{article}', '') as article,
        nullif(raw_record #>> '{articleSubstitution}', '') as article_substitution,
        staging.try_bigint(raw_record #>> '{barcode}') as barcode,
        nullif(raw_record #>> '{bonusTypeName}', '') as bonus_type_name,
        nullif(raw_record #>> '{brandName}', '') as brand_name,
        nullif(raw_record #>> '{cashbackAmount}', '') as cashback_amount,
        nullif(raw_record #>> '{cashbackCommissionChange}', '') as cashback_commission_change,
        nullif(raw_record #>> '{cashbackDiscount}', '') as cashback_discount,
        staging.try_bigint(raw_record #>> '{chrtId}') as chrt_id,
        staging.try_numeric(raw_record #>> '{commissionPercent}') as commission_percent,
        staging.try_bigint(raw_record #>> '{convertedCurrencyCode}') as converted_currency_code,
        staging.try_bigint(raw_record #>> '{convertedFinalPrice}') as converted_final_price,
        staging.try_bigint(raw_record #>> '{convertedPrice}') as converted_price,
        nullif(raw_record #>> '{country}', '') as country,
        staging.try_timestamptz(raw_record #>> '{createDate}') as create_date,
        staging.try_timestamptz(raw_record #>> '{createdAt}') as created_at,
        nullif(raw_record #>> '{currency}', '') as currency,
        staging.try_bigint(raw_record #>> '{currencyCode}') as currency_code,
        staging.try_timestamptz(raw_record #>> '{dateFrom}') as date_from,
        staging.try_timestamptz(raw_record #>> '{dateTo}') as date_to,
        nullif(raw_record #>> '{declarationNumber}', '') as declaration_number,
        nullif(raw_record #>> '{deduction}', '') as deduction,
        staging.try_bigint(raw_record #>> '{deliveryAmount}') as delivery_amount,
        nullif(raw_record #>> '{deliveryMethod}', '') as delivery_method,
        nullif(raw_record #>> '{deliveryService}', '') as delivery_service,
        staging.try_numeric(raw_record #>> '{dlvPrc}') as dlv_prc,
        nullif(raw_record #>> '{docTypeName}', '') as doc_type_name,
        staging.try_bigint(raw_record #>> '{finalPrice}') as final_price,
        staging.try_timestamptz(raw_record #>> '{fixTariffDateFrom}') as fix_tariff_date_from,
        staging.try_timestamptz(raw_record #>> '{fixTariffDateTo}') as fix_tariff_date_to,
        nullif(raw_record #>> '{forPay}', '') as for_pay,
        nullif(raw_record #>> '{giBoxTypeName}', '') as gi_box_type_name,
        staging.try_bigint(raw_record #>> '{giId}') as gi_id,
        staging.try_bigint(raw_record #>> '{id}') as id,
        nullif(raw_record #>> '{installmentCofinancingAmount}', '') as installment_cofinancing_amount,
        staging.try_bool(raw_record #>> '{isB2b}') as is_b2b,
        staging.try_numeric(raw_record #>> '{isKgvpV2}') as is_kgvp_v2,
        nullif(raw_record #>> '{kiz}', '') as kiz,
        staging.try_numeric(raw_record #>> '{kvw}') as kvw,
        staging.try_numeric(raw_record #>> '{kvwBase}') as kvw_base,
        staging.try_numeric(raw_record #>> '{loyaltyDiscount}') as loyalty_discount,
        staging.try_bigint(raw_record #>> '{loyaltyId}') as loyalty_id,
        staging.try_bigint(raw_record #>> '{nmId}') as nm_id,
        staging.try_bigint(raw_record #>> '{officeId}') as office_id,
        nullif(raw_record #>> '{officeName}', '') as office_name,
        staging.try_timestamptz(raw_record #>> '{operationDate}') as operation_date,
        staging.try_timestamptz(raw_record #>> '{orderDt}') as order_dt,
        staging.try_bigint(raw_record #>> '{orderId}') as order_id,
        nullif(raw_record #>> '{orderUid}', '') as order_uid,
        nullif(raw_record #>> '{paidAcceptance}', '') as paid_acceptance,
        nullif(raw_record #>> '{paidStorage}', '') as paid_storage,
        nullif(raw_record #>> '{paymentProcessing}', '') as payment_processing,
        nullif(raw_record #>> '{paymentSchedule}', '') as payment_schedule,
        nullif(raw_record #>> '{penalty}', '') as penalty,
        staging.try_bigint(raw_record #>> '{ppvzOfficeId}') as ppvz_office_id,
        nullif(raw_record #>> '{ppvzOfficeName}', '') as ppvz_office_name,
        nullif(raw_record #>> '{ppvzReward}', '') as ppvz_reward,
        nullif(raw_record #>> '{ppvzSalesCommission}', '') as ppvz_sales_commission,
        nullif(raw_record #>> '{ppvzSupplierInn}', '') as ppvz_supplier_inn,
        nullif(raw_record #>> '{ppvzSupplierName}', '') as ppvz_supplier_name,
        staging.try_bigint(raw_record #>> '{price}') as price,
        staging.try_numeric(raw_record #>> '{productDiscountForReport}') as product_discount_for_report,
        staging.try_bigint(raw_record #>> '{quantity}') as quantity,
        nullif(raw_record #>> '{rebillLogisticCost}', '') as rebill_logistic_cost,
        nullif(raw_record #>> '{rebillLogisticOrg}', '') as rebill_logistic_org,
        staging.try_bigint(raw_record #>> '{reportId}') as report_id,
        staging.try_bigint(raw_record #>> '{reportType}') as report_type,
        nullif(raw_record #>> '{retailAmount}', '') as retail_amount,
        nullif(raw_record #>> '{retailPrice}', '') as retail_price,
        nullif(raw_record #>> '{retailPriceWithDisc}', '') as retail_price_with_disc,
        staging.try_bigint(raw_record #>> '{returnAmount}') as return_amount,
        nullif(raw_record #>> '{rid}', '') as rid,
        staging.try_timestamptz(raw_record #>> '{rrDate}') as rr_date,
        staging.try_bigint(raw_record #>> '{rrdId}') as rrd_id,
        staging.try_timestamptz(raw_record #>> '{saleDt}') as sale_dt,
        staging.try_bigint(raw_record #>> '{salePercent}') as sale_percent,
        staging.try_bigint(raw_record #>> '{salePrice}') as sale_price,
        staging.try_bigint(raw_record #>> '{salePriceAffiliatedDiscountPrc}') as sale_price_affiliated_discount_prc,
        staging.try_bigint(raw_record #>> '{salePricePromocodeDiscountPrc}') as sale_price_promocode_discount_prc,
        staging.try_bigint(raw_record #>> '{salePriceWholesaleDiscountPrc}') as sale_price_wholesale_discount_prc,
        nullif(raw_record #>> '{sellerOperName}', '') as seller_oper_name,
        nullif(raw_record #>> '{sellerPromo}', '') as seller_promo,
        staging.try_numeric(raw_record #>> '{sellerPromoDiscount}') as seller_promo_discount,
        staging.try_bigint(raw_record #>> '{sellerPromoId}') as seller_promo_id,
        staging.try_bigint(raw_record #>> '{shkId}') as shk_id,
        staging.try_bigint(raw_record #>> '{sku}') as sku,
        raw_record #> '{skus}' as skus,
        staging.try_numeric(raw_record #>> '{spp}') as spp,
        nullif(raw_record #>> '{srid}', '') as srid,
        staging.try_bool(raw_record #>> '{srvDbs}') as srv_dbs,
        nullif(raw_record #>> '{stickerId}', '') as sticker_id,
        nullif(raw_record #>> '{subjectName}', '') as subject_name,
        staging.try_numeric(raw_record #>> '{supRatingUp}') as sup_rating_up,
        nullif(raw_record #>> '{techSize}', '') as tech_size,
        nullif(raw_record #>> '{title}', '') as title,
        nullif(raw_record #>> '{trbxId}', '') as trbx_id,
        nullif(raw_record #>> '{uuidPromocode}', '') as uuid_promocode,
        nullif(raw_record #>> '{vendorCode}', '') as vendor_code,
        nullif(raw_record #>> '{vw}', '') as vw,
        nullif(raw_record #>> '{vwNds}', '') as vw_nds,
        nullif(raw_record #>> '{warehouseAddress}', '') as warehouse_address,
        staging.try_bigint(raw_record #>> '{warehouseId}') as warehouse_id,
        nullif(raw_record #>> '{warehouseName}', '') as warehouse_name,
        staging.try_numeric(raw_record #>> '{wibesDiscountPercent}') as wibes_discount_percent
    from expanded

)

select *
from typed
