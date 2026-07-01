{{ config(materialized='table', schema='staging_cleaned', alias='fbw_supplies', tags=['staging_cleaned']) }}

select *
from {{ ref('v_fbw_supplies_for_cleaned') }}
