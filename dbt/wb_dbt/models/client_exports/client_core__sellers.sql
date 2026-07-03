{{ config(alias='core__sellers', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('sellers') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
