{{ config(alias='marts__mart_orders_daily', tags=['client_exports', 'client_marts']) }}

select *
from {{ ref('mart_orders_daily') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
