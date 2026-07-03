{{ config(alias='core__product_variants', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('product_variants') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
