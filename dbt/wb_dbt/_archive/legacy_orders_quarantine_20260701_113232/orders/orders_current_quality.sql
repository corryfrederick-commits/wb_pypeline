{{ config(materialized='table', schema='quarantine') }}

with source_orders as (

    select *
    from {{ ref('stg_orders_current') }}

),

issues as (

    select *
    from {{ ref('orders_current_issues') }}

)

select
    s.raw_payload_id,
    s.record_index,
    s.dataset_name,
    s.source_file,
    s.order_flow,
    s.order_kind,
    s.order_id,

    case
        when count(i.id) filter (where i.issue_level = 'bad') > 0 then 'bad'
        when count(i.id) filter (where i.issue_level = 'partial') > 0 then 'partial'
        else 'good'
    end as quality_status,

    coalesce(
        array_agg(i.issue_code order by i.issue_code)
            filter (where i.issue_level in ('bad', 'partial')),
        array[]::text[]
    ) as quality_issues,

    coalesce(
        array_agg(i.issue_code order by i.issue_code)
            filter (where i.issue_level = 'warning'),
        array[]::text[]
    ) as warning_issues,

    coalesce(
        jsonb_agg(
            jsonb_build_object(
                'issue_level', i.issue_level,
                'issue_code', i.issue_code,
                'problem_field', i.problem_field,
                'problem_value', i.problem_value,
                'details', i.details
            )
            order by i.issue_level, i.issue_code
        ) filter (where i.id is not null),
        '[]'::jsonb
    ) as issue_details,

    case
        when count(i.id) filter (where i.issue_level = 'bad') > 0 then false
        else true
    end as can_load_to_core,

    case
        when count(i.id) filter (where i.issue_level = 'bad') > 0 then false
        else true
    end as can_count_order,

    case
        when count(i.id) filter (
            where i.issue_code in ('missing_price', 'negative_price', 'final_price_greater_than_price')
        ) > 0 then false
        when count(i.id) filter (where i.issue_level = 'bad') > 0 then false
        else true
    end as can_count_revenue,

    case
        when count(i.id) filter (where i.issue_code = 'missing_created_at') > 0 then false
        when count(i.id) filter (where i.issue_level = 'bad') > 0 then false
        else true
    end as can_use_order_date,

    case
        when count(i.id) filter (where i.issue_level in ('bad', 'partial')) > 0 then false
        else true
    end as is_complete,

    now() as checked_at

from source_orders s
left join issues i
  on i.raw_payload_id = s.raw_payload_id
 and i.record_index = s.record_index
group by
    s.raw_payload_id,
    s.record_index,
    s.dataset_name,
    s.source_file,
    s.order_flow,
    s.order_kind,
    s.order_id
