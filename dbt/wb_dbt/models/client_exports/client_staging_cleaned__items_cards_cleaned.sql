{{ config(alias='staging_cleaned__items_cards_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('items_cards_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
