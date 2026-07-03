{{ config(alias='core__tariff_acceptance_prices', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('tariff_acceptance_prices') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
