{{ config(alias='staging_cleaned__report_orders_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('report_orders_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
