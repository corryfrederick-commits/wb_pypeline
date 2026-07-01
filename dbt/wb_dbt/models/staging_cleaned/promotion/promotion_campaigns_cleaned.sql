{{ config(materialized='table', schema='staging_cleaned', alias='promotion_campaigns', tags=['staging_cleaned']) }}

select *
from {{ ref('v_promotion_campaigns_for_cleaned') }}
