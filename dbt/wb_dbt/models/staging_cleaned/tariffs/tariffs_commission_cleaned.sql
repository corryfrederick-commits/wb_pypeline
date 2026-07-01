{{ config(materialized='table', schema='staging_cleaned', alias='tariffs_commission', tags=['staging_cleaned']) }}

select *
from {{ ref('v_tariffs_commission_for_cleaned') }}
