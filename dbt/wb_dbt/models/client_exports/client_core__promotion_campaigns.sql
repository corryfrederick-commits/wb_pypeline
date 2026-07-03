{{ config(alias='core__promotion_campaigns', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('promotion_campaigns') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
