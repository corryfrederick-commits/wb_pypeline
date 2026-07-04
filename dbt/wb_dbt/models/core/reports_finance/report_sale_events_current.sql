{{ config(materialized='view', schema='core', alias='report_sale_events_current', tags=['core', 'core_reports_finance', 'current']) }}

select *
from {{ ref('report_sale_events') }}
where is_current = true
