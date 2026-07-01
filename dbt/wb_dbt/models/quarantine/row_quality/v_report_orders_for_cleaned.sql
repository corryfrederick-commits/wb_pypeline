{{ config(materialized='view', schema='quarantine', alias='v_report_orders_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_report_orders_quality') }}
where can_load_to_cleaned = true
