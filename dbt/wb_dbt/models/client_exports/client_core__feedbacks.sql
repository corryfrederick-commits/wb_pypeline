{{ config(alias='core__feedbacks', tags=['client_exports', 'client_core']) }}

select *
from {{ ref('feedbacks') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
