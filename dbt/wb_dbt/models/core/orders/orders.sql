{{ config(materialized='table', schema='core') }}

with source_orders as (

    select *
    from {{ ref('v_orders_current_for_core') }}

),

final as (

    select
        md5(
            'orders|' ||
            raw_payload_id::text ||
            '|' ||
            record_index::text
        ) as order_row_id,

        coalesce(
            order_id::text,
            nullif(order_uid, ''),
            nullif(rid, ''),
            nullif(order_code, '')
        ) as order_identity,

        raw_payload_id,
        record_index,

        source_system,
        dataset_name,
        source_file,
        source_url,
        file_hash,
        loaded_at,

        order_flow,
        order_kind,
        is_archive,

        order_id,
        rid,
        srid,
        order_uid,
        order_code,

        created_at,
        created_at::date as order_date,
        ddate,
        seller_date,

        delivery_type,
        delivery_method,
        delivery_service,
        pay_mode,

        article,
        nm_id,
        chrt_id,
        barcode,
        skus,

        price as price_raw,
        sale_price as sale_price_raw,
        final_price as final_price_raw,
        converted_price as converted_price_raw,
        converted_final_price as converted_final_price_raw,
        currency_code,
        converted_currency_code,
        scan_price as scan_price_raw,

        warehouse_id,
        warehouse_address,
        office_id,
        supply_id,

        address_full,
        address_latitude,
        address_longitude,

        group_id,
        cargo_type,
        cross_border_type,
        color_code,
        comment,

        is_zero_order,
        is_b2b,

        quality_status,
        quality_issues,
        warning_issues,
        issue_details,
        can_load_to_core,
        can_count_order,
        can_count_revenue,
        can_use_order_date,
        is_complete,

        raw_record,

        now() as core_loaded_at

    from source_orders

)

select *
from final
