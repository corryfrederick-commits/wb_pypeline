{{ config(alias='marts__mart_stock_current', tags=['client_exports', 'client_marts']) }}

select *
from {{ ref('mart_stock_current') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
