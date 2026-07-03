{{ config(alias='core__sales_funnel_metrics', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('sales_funnel_metrics') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
