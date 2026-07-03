{{ config(alias='staging_cleaned__communications_feedbacks_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('communications_feedbacks_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
