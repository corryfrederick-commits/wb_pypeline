{{ config(alias='core__product_barcodes', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('product_barcodes') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
