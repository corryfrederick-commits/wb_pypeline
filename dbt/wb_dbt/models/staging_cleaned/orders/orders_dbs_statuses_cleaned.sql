{{ config(materialized='table', schema='staging_cleaned', alias='orders_dbs_statuses', tags=['staging_cleaned']) }}

select *
from {{ ref('v_orders_dbs_statuses_for_cleaned') }}
