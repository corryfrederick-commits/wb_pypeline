{{ config(materialized='table', schema='staging_cleaned', alias='promotion_fullstats', tags=['staging_cleaned']) }}

select *
from {{ ref('v_promotion_fullstats_for_cleaned') }}
