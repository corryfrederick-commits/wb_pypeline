{{ config(alias='core__order_status_snapshots', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('order_status_snapshots') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
