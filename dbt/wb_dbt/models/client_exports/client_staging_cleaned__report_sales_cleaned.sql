{{ config(alias='staging_cleaned__report_sales_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('report_sales_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
