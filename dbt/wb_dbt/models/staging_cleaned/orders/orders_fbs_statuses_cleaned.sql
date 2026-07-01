{{ config(materialized='table', schema='staging_cleaned', alias='orders_fbs_statuses', tags=['staging_cleaned']) }}

select *
from {{ ref('v_orders_fbs_statuses_for_cleaned') }}
