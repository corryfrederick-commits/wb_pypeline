{{ config(materialized='table', schema='staging_cleaned', alias='report_orders', tags=['staging_cleaned']) }}

select *
from {{ ref('v_report_orders_for_cleaned') }}
