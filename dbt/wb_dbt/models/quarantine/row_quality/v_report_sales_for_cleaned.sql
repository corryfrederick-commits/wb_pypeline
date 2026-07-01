{{ config(materialized='view', schema='quarantine', alias='v_report_sales_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_report_sales_quality') }}
where can_load_to_cleaned = true
