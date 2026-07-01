{{ config(materialized='table', schema='staging_cleaned', alias='finance_balance', tags=['staging_cleaned']) }}

select *
from {{ ref('v_finance_balance_for_cleaned') }}
