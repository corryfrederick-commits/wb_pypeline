{{ config(materialized='table', schema='staging_cleaned', alias='report_sales', tags=['staging_cleaned']) }}

select *
from {{ ref('v_report_sales_for_cleaned') }}
