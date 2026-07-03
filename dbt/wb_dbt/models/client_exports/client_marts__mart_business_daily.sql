{{ config(alias='marts__mart_business_daily', tags=['client_exports', 'client_marts']) }}

select *
from {{ ref('mart_business_daily') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
