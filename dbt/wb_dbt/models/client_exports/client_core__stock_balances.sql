{{ config(alias='core__stock_balances', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('stock_balances') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
