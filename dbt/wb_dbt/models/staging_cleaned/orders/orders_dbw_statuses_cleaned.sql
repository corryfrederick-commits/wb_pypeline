{{ config(materialized='table', schema='staging_cleaned', alias='orders_dbw_statuses', tags=['staging_cleaned']) }}

select *
from {{ ref('v_orders_dbw_statuses_for_cleaned') }}
