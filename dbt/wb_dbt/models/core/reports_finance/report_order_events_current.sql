{{ config(materialized='view', schema='core', alias='report_order_events_current', tags=['core', 'core_reports_finance', 'current']) }}

select *
from {{ ref('report_order_events') }}
where is_current = true
