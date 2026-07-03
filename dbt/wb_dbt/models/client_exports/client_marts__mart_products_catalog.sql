{{ config(alias='marts__mart_products_catalog', tags=['client_exports', 'client_marts']) }}

select *
from {{ ref('mart_products_catalog') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
