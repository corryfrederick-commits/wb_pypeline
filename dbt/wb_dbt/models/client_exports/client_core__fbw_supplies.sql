{{ config(alias='core__fbw_supplies', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('fbw_supplies') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
