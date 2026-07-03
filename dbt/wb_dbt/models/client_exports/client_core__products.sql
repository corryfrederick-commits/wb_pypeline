{{ config(alias='core__products', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('products') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
