{{ config(materialized='table', schema='staging_cleaned', alias='tariffs_box', tags=['staging_cleaned']) }}

select *
from {{ ref('v_tariffs_box_for_cleaned') }}
