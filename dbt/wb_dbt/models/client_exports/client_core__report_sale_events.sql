{{ config(alias='core__report_sale_events', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('report_sale_events') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
