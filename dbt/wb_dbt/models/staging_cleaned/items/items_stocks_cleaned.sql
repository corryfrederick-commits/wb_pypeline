{{ config(materialized='table', schema='staging_cleaned', alias='items_stocks', tags=['staging_cleaned']) }}

select *
from {{ ref('v_items_stocks_for_cleaned') }}
