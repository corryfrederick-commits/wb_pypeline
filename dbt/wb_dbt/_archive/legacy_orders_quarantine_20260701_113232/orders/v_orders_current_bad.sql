{{ config(materialized='view', schema='quarantine') }}

select
    s.*,
    q.quality_status,
    q.quality_issues,
    q.warning_issues,
    q.issue_details,
    q.can_load_to_core,
    q.can_count_order,
    q.can_count_revenue,
    q.can_use_order_date,
    q.is_complete
from {{ ref('stg_orders_current') }} s
join {{ ref('orders_current_quality') }} q
  on q.raw_payload_id = s.raw_payload_id
 and q.record_index = s.record_index
where q.quality_status = 'bad'
