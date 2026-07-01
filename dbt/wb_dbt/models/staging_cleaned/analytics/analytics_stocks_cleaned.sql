{{ config(materialized='table', schema='staging_cleaned', alias='analytics_stocks', tags=['staging_cleaned']) }}

select *
from {{ ref('v_analytics_stocks_for_cleaned') }}
