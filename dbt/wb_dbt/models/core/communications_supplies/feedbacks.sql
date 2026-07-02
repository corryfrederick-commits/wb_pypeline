{{ config(materialized='table', schema='core', alias='feedbacks', tags=['core', 'core_communications_supplies']) }}

with source as (

    select *
    from {{ ref('communications_feedbacks_cleaned') }}
    where can_load_to_cleaned = true

)

select
    client_id,
    wb_account_id,
    md5(concat_ws(
        '||',
        'feedback',
        raw_payload_id::text,
        record_index::text
    )) as feedback_key,

    id as source_feedback_id,
    parent_feedback_id,
    child_feedback_id,

    created_date as feedback_created_at,
    state as feedback_state,
    answer_state,
    was_viewed,

    user_name,
    text as feedback_text,
    pros,
    cons,
    matching_size,
    color,

    product_valuation,
    supplier_feedback_valuation,
    supplier_product_valuation,

    answer_text,
    answer_editable,

    product_details_nm_id as product_id,
    product_details_nm_id as nm_id,
    product_details_imt_id as imt_id,
    product_details_product_name as product_name,
    product_details_brand_name as brand_name,
    product_details_supplier_article as supplier_article,
    product_details_supplier_name as supplier_name,
    product_details_size as product_size,

    subject_id,
    subject_name,

    order_status,
    last_order_created_at,
    last_order_shk_id,

    is_able_return_product_orders,
    is_able_supplier_feedback_valuation,
    is_able_supplier_product_valuation,
    return_product_orders_date,

    photo_links,
    video_link,
    video_preview_image,
    video_duration_sec,
    bables,

    source_system,
    dataset_name as source_dataset,
    md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
    raw_payload_id,
    record_index,
    loaded_at as source_loaded_at,
    now() as core_loaded_at

from source
