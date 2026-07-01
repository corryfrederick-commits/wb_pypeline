{{ config(materialized='view', schema='quarantine', alias='v_finance_sales_reports_detailed_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_finance_sales_reports_detailed_quality') }}
where can_load_to_cleaned = true
