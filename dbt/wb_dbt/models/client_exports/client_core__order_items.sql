{{ config(alias='core__order_items', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('order_items') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
