{{ config(alias='staging_cleaned__finance_sales_reports_detailed_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('finance_sales_reports_detailed_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
