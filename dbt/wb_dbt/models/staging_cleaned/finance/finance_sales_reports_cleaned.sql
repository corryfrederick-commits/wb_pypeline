{{ config(materialized='table', schema='staging_cleaned', alias='finance_sales_reports', tags=['staging_cleaned']) }}

select *
from {{ ref('v_finance_sales_reports_for_cleaned') }}
