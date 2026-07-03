{{ config(alias='core__stock_analytics_metrics', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('stock_analytics_metrics') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
