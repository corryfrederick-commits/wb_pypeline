{{ config(alias='core__chats', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('chats') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
