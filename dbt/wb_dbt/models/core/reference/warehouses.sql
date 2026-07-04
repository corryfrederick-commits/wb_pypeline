{{ config(materialized='table', schema='core', alias='warehouses', tags=['core', 'core_reference']) }}

with source as (

    select *
    from {{ ref('items_warehouses_cleaned') }}
    where can_load_to_cleaned = true

),

prepared as (

    select
        *,
        coalesce(id, warehouse_id, office_id) as warehouse_natural_id,
        coalesce(nullif(name, ''), nullif(warehouse_name, '')) as resolved_warehouse_name,
        coalesce(nullif(warehouse_address, ''), null) as resolved_warehouse_address
    from source
    where coalesce(id, warehouse_id, office_id) is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by client_id, wb_account_id, warehouse_natural_id
            order by loaded_at desc, raw_payload_id desc, record_index desc
        ) as rn
    from prepared

),

real_rows as (

    select
        client_id,
        wb_account_id,
        md5(concat_ws('||', client_id, wb_account_id, 'warehouse', warehouse_natural_id::text)) as warehouse_key,

        warehouse_natural_id,
        warehouse_natural_id as warehouse_id,
        office_id,
        id as source_warehouse_id,

        resolved_warehouse_name as warehouse_name,
        nullif(name, '') as source_name,
        resolved_warehouse_address as warehouse_address,

        delivery_type,
        cargo_type,
        is_deleting,
        is_processing,

        source_system,
        dataset_name as source_dataset,
        md5(concat_ws('||', client_id, wb_account_id, raw_payload_id::text, record_index::text)) as source_row_id,
        raw_payload_id,
        record_index,
        loaded_at as source_loaded_at,
        now() as core_loaded_at

    from deduplicated
    where rn = 1

),

fact_warehouse_keys as (

    select client_id, wb_account_id, warehouse_key
    from {{ ref('report_order_events') }}
    where warehouse_key is not null

    union

    select client_id, wb_account_id, warehouse_key
    from {{ ref('report_sale_events') }}
    where warehouse_key is not null

),

inferred_rows as (

    select * from real_rows where false

    union all

    select
        f.client_id,
        f.wb_account_id,
        f.warehouse_key,

        abs(('x' || substr(md5(f.warehouse_key::text), 1, 15))::bit(60)::bigint) as warehouse_natural_id,
        abs(('x' || substr(md5(f.warehouse_key::text), 1, 15))::bit(60)::bigint) as warehouse_id,
        null as office_id,
        null as source_warehouse_id,

        'INFERRED warehouse ' || f.warehouse_key::text as warehouse_name,
        null as source_name,
        null as warehouse_address,

        null as delivery_type,
        null as cargo_type,
        null as is_deleting,
        null as is_processing,

        'inferred' as source_system,
        'inferred_from_fact_keys' as source_dataset,
        md5(concat_ws('||', f.client_id, f.wb_account_id, 'inferred_warehouse', f.warehouse_key::text)) as source_row_id,
        null as raw_payload_id,
        null as record_index,
        null as source_loaded_at,
        now() as core_loaded_at

    from fact_warehouse_keys f
    left join real_rows r
        on f.client_id = r.client_id
       and f.wb_account_id = r.wb_account_id
       and f.warehouse_key = r.warehouse_key
    where r.warehouse_key is null

)

select * from real_rows
union all
select * from inferred_rows
