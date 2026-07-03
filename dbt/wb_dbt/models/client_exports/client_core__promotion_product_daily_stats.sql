{{ config(alias='core__promotion_product_daily_stats', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('promotion_product_daily_stats') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
