{{ config(alias='core__finance_transactions', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('finance_transactions') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
