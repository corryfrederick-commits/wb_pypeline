{{ config(alias='staging_cleaned__analytics_stocks_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('analytics_stocks_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
