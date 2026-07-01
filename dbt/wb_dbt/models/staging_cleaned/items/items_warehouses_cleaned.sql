{{ config(materialized='table', schema='staging_cleaned', alias='items_warehouses', tags=['staging_cleaned']) }}

select *
from {{ ref('v_items_warehouses_for_cleaned') }}
