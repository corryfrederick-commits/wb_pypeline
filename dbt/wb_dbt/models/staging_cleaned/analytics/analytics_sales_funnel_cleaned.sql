{{ config(materialized='table', schema='staging_cleaned', alias='analytics_sales_funnel', tags=['staging_cleaned']) }}

select *
from {{ ref('v_analytics_sales_funnel_for_cleaned') }}
