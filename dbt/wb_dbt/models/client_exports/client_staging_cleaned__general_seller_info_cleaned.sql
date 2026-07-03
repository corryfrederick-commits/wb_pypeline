{{ config(alias='staging_cleaned__general_seller_info_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('general_seller_info_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
