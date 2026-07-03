{{ config(alias='core__finance_report_summaries', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('finance_report_summaries') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
