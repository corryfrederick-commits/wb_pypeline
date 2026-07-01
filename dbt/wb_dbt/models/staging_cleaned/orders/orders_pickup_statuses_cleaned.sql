{{ config(materialized='table', schema='staging_cleaned', alias='orders_pickup_statuses', tags=['staging_cleaned']) }}

select *
from {{ ref('v_orders_pickup_statuses_for_cleaned') }}
