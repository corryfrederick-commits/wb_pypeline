{{ config(materialized='table', schema='staging', tags=['auto_staging']) }}

with latest_raw as (

    select distinct on (source_system, dataset_name, source_file)
        id as raw_payload_id,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        payload
    from {{ source('quarantine', 'v_raw_payloads_schema_passed') }}
    where dataset_name = 'communications_feedbacks'
    order by
        source_system,
        dataset_name,
        source_file,
        loaded_at desc,
        id desc

),

expanded as (

    select
        p.raw_payload_id,
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
            when jsonb_typeof(p.payload #> '{data,feedbacks}') = 'array' then p.payload #> '{data,feedbacks}'
            when jsonb_typeof(p.payload #> '{data,feedbacks}') = 'object' then jsonb_build_array(p.payload #> '{data,feedbacks}')
            else '[]'::jsonb
        end
    ) with ordinality as x(raw_record, ordinality)

),

typed as (

    select
        raw_payload_id,
        record_index,
        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,
        raw_record,
        staging.try_bool(raw_record #>> '{answer,editable}') as answer_editable,
        nullif(raw_record #>> '{answer,state}', '') as answer_state,
        nullif(raw_record #>> '{answer,text}', '') as answer_text,
        raw_record #> '{bables}' as bables,
        nullif(raw_record #>> '{childFeedbackId}', '') as child_feedback_id,
        nullif(raw_record #>> '{color}', '') as color,
        nullif(raw_record #>> '{cons}', '') as cons,
        staging.try_timestamptz(raw_record #>> '{createdDate}') as created_date,
        nullif(raw_record #>> '{id}', '') as id,
        staging.try_bool(raw_record #>> '{isAbleReturnProductOrders}') as is_able_return_product_orders,
        staging.try_bool(raw_record #>> '{isAbleSupplierFeedbackValuation}') as is_able_supplier_feedback_valuation,
        staging.try_bool(raw_record #>> '{isAbleSupplierProductValuation}') as is_able_supplier_product_valuation,
        nullif(raw_record #>> '{lastOrderCreatedAt}', '') as last_order_created_at,
        staging.try_bigint(raw_record #>> '{lastOrderShkId}') as last_order_shk_id,
        nullif(raw_record #>> '{matchingSize}', '') as matching_size,
        nullif(raw_record #>> '{orderStatus}', '') as order_status,
        nullif(raw_record #>> '{parentFeedbackId}', '') as parent_feedback_id,
        raw_record #> '{photoLinks}' as photo_links,
        nullif(raw_record #>> '{productDetails,brandName}', '') as product_details_brand_name,
        staging.try_bigint(raw_record #>> '{productDetails,imtId}') as product_details_imt_id,
        staging.try_bigint(raw_record #>> '{productDetails,nmId}') as product_details_nm_id,
        nullif(raw_record #>> '{productDetails,productName}', '') as product_details_product_name,
        nullif(raw_record #>> '{productDetails,size}', '') as product_details_size,
        nullif(raw_record #>> '{productDetails,supplierArticle}', '') as product_details_supplier_article,
        nullif(raw_record #>> '{productDetails,supplierName}', '') as product_details_supplier_name,
        staging.try_bigint(raw_record #>> '{productValuation}') as product_valuation,
        nullif(raw_record #>> '{pros}', '') as pros,
        staging.try_timestamptz(raw_record #>> '{returnProductOrdersDate}') as return_product_orders_date,
        nullif(raw_record #>> '{state}', '') as state,
        staging.try_bigint(raw_record #>> '{subjectId}') as subject_id,
        nullif(raw_record #>> '{subjectName}', '') as subject_name,
        staging.try_bigint(raw_record #>> '{supplierFeedbackValuation}') as supplier_feedback_valuation,
        staging.try_bigint(raw_record #>> '{supplierProductValuation}') as supplier_product_valuation,
        nullif(raw_record #>> '{text}', '') as text,
        nullif(raw_record #>> '{userName}', '') as user_name,
        staging.try_bigint(raw_record #>> '{video,durationSec}') as video_duration_sec,
        nullif(raw_record #>> '{video,link}', '') as video_link,
        nullif(raw_record #>> '{video,previewImage}', '') as video_preview_image,
        staging.try_bool(raw_record #>> '{wasViewed}') as was_viewed
    from expanded

)

select *
from typed
