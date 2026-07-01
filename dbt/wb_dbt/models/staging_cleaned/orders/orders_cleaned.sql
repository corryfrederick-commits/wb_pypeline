{{ config(materialized='table', schema='staging_cleaned', alias='orders', tags=['staging_cleaned']) }}

select *
from {{ ref('v_orders_for_cleaned') }}
