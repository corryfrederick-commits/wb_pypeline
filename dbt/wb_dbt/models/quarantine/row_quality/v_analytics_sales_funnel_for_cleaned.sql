{{ config(materialized='view', schema='quarantine', alias='v_analytics_sales_funnel_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_analytics_sales_funnel_quality') }}
where can_load_to_cleaned = true
