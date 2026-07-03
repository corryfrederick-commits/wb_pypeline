{{ config(alias='staging_cleaned__promotion_fullstats_cleaned', tags=['client_exports', 'client_staging_cleaned']) }}

select *
from {{ ref('promotion_fullstats_cleaned') }}
where client_id = 'demo_client'
  and wb_account_id = 'demo_wb_account'
