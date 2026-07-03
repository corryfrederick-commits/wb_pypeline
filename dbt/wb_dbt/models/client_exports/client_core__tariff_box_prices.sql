{{ config(alias='core__tariff_box_prices', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('tariff_box_prices') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
